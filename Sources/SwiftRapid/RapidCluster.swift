import Foundation
import NIO
import Logging

/// TODO documentation
/// TODO leaving
/// TODO listener for shutdown
final class RapidCluster {
    private let membershipService: MembershipService
    private let messagingServer: MessagingServer
    private let listenAddress: Endpoint
    private var hasShutdown = false

    private init(messagingServer: MessagingServer,
                 membershipService: MembershipService,
                 listenAddress: Endpoint) {
        self.membershipService = membershipService
        self.messagingServer = messagingServer
        self.listenAddress = listenAddress
    }

    func getMemberList() throws -> [Endpoint] {
        try checkIfRunning()
        return try membershipService.getMemberList()
    }

    func getClusterMetadata() throws -> [Endpoint: Metadata] {
        try checkIfRunning()
        return try membershipService.getMetadata()
    }

    func leaveGracefully() throws {
        fatalError("Not implemented")
    }

    func shutdown() throws {
        fatalError("Not implemented")
    }

    struct Builder {
        private let logger = Logger(label: "rapid.RapidCluster")

        private let joinAttempts = 5

        public var host: String = ""
        public var port: Int = 0
        public var metadata: Metadata = Metadata()
        public var edgeFailureDetectorProvider: EdgeFailureDetectorProvider? = nil
        public var settings: Settings = Settings()
        public var messagingClient: MessagingClient? = nil
        public var messagingServer: MessagingServer? = nil
        private var eventSubscriptions: [(ClusterEvent) -> ()] = []

        public static func with(
                _ populator: (inout Self) throws -> ()
        ) rethrows -> Self {
            var builder = Self()
            try populator(&builder)
            return builder
        }

        mutating func registerSubscription(callback: @escaping (ClusterEvent) -> ()) {
            eventSubscriptions.append(callback)
        }

        func start() throws -> RapidCluster {
            precondition(host != "", "host is not set")
            precondition(port != 0, "port is not set")
            let selfEndpoint = addressFromParts(host, port)
            // TODO configurable
            let serverGroup = MultiThreadedEventLoopGroup(numberOfThreads: 4)
            let clientGroup = MultiThreadedEventLoopGroup(numberOfThreads: 4)
            let messagingServer: MessagingServer = self.messagingServer ?? GrpcMessagingServer(address: selfEndpoint, group: serverGroup)
            let messagingClient: MessagingClient = self.messagingClient ?? GrpcMessagingClient(group: clientGroup, settings: settings)
            let broadcaster = UnicastToAllBroadcaster(client: messagingClient)
            let currentIdentifier = nodeIdFromUUID(UUID())
            // TODO should also be assigned a group and be the one to hand out event loops
            let actorRefProvider = ActorRefProvider()
            let edgeFailureDetectorProvider = self.edgeFailureDetectorProvider ?? AdaptiveAccrualFailureDetectorProvider(selfEndpoint: selfEndpoint, messagingClient: messagingClient, provider: actorRefProvider, el: clientGroup.next())
            let membershipView = MembershipView(K: self.settings.K, nodeIds: [currentIdentifier], endpoints: [selfEndpoint])
            let membershipService = try RapidMembershipService(
                    selfEndpoint: selfEndpoint,
                    settings: settings,
                    view: membershipView,
                    failureDetectorProvider: edgeFailureDetectorProvider,
                    broadcaster: broadcaster,
                    messagingClient: messagingClient,
                    allMetadata: [selfEndpoint: metadata],
                    subscriptions: eventSubscriptions,
                    provider: actorRefProvider,
                    el: clientGroup.next()
            )
            messagingServer.onMembershipServiceInitialized(membershipService: membershipService)
            try messagingServer.start()
            logger.info("Successfully started Rapid cluster")
            return RapidCluster(messagingServer: messagingServer, membershipService: membershipService, listenAddress: selfEndpoint)
        }

        func join(host: String, port: Int) throws -> RapidCluster {
            precondition(self.host != "", "host is not set")
            precondition(self.port != 0, "port is not set")
            let listenAddress = addressFromParts(self.host, self.port)
            let seedAddress = addressFromParts(host, port)
            var currentIdentifier = nodeIdFromUUID(UUID())
            // TODO configurable
            let serverGroup = MultiThreadedEventLoopGroup(numberOfThreads: 4)
            let clientGroup = MultiThreadedEventLoopGroup(numberOfThreads: 4)
            let messagingServer: MessagingServer = self.messagingServer ?? GrpcMessagingServer(address: listenAddress, group: serverGroup)
            let messagingClient: MessagingClient = self.messagingClient ?? GrpcMessagingClient(group: clientGroup, settings: settings)
            let broadcaster = UnicastToAllBroadcaster(client: messagingClient)
            // TODO should also be assigned a group and be the one to hand out event loops
            let actorRefProvider = ActorRefProvider()
            let edgeFailureDetectorProvider = self.edgeFailureDetectorProvider ?? AdaptiveAccrualFailureDetectorProvider(selfEndpoint: listenAddress, messagingClient: messagingClient, provider: actorRefProvider, el: clientGroup.next())

            func joinAttempt(seedAddress: Endpoint, listenAddress: Endpoint, nodeId: NodeId, attempt: Int) throws -> RapidCluster {
                let joinRequest = RapidRequest.with {
                    $0.joinMessage = JoinMessage.with {
                        $0.sender = listenAddress
                        $0.nodeID = nodeId
                        $0.metadata = metadata
                    }
                }
                let joinResponse = try messagingClient.sendMessage(recipient: seedAddress, msg: joinRequest).wait().joinResponse
                if (joinResponse.statusCode != JoinStatusCode.safeToJoin) {
                    throw RapidClusterError.joinError(joinResponse)
                }
                return try createClusterFromJoinResponse(selfEndpoint: listenAddress, response: joinResponse)
            }

            func createClusterFromJoinResponse(selfEndpoint: Endpoint, response: JoinResponse) throws -> RapidCluster {
                var allMetadata = [Endpoint: Metadata]()
                for i in 0..<response.metadataKeys.count {
                    allMetadata[response.metadataKeys[i]] = response.metadataValues[i]
                }

                let membershipView = MembershipView(K: settings.K, nodeIds: response.identifiers, endpoints: response.endpoints)
                let membershipService = try RapidMembershipService(
                        selfEndpoint: selfEndpoint,
                        settings: settings,
                        view: membershipView,
                        failureDetectorProvider: edgeFailureDetectorProvider,
                        broadcaster: broadcaster,
                        messagingClient: messagingClient,
                        allMetadata: [selfEndpoint: metadata],
                        subscriptions: eventSubscriptions,
                        provider: actorRefProvider,
                        el: clientGroup.next()
                )
                messagingServer.onMembershipServiceInitialized(membershipService: membershipService)
                try messagingServer.start()
                logger.info("Successfully joined Rapid cluster with \(response.endpoints.count) members")
                return RapidCluster(messagingServer: messagingServer, membershipService: membershipService, listenAddress: selfEndpoint)
            }

            for attempt in 0..<joinAttempts {
                do {
                    return try joinAttempt(seedAddress: seedAddress, listenAddress: listenAddress, nodeId: currentIdentifier, attempt: attempt)
                } catch RapidClusterError.joinError(let joinResponse) {
                    switch joinResponse.statusCode {
                        case .uuidAlreadyInRing:
                            logger.error("Node with the same UUID already present. Retrying.")
                            currentIdentifier = nodeIdFromUUID(UUID())
                            break
                        case .hostnameAlreadyInRing:
                            logger.error("Membership rejected, retrying.")
                            break
                        case .viewChangeInProgress:
                            logger.error("Seed node is executing a view change, retrying.")
                            break
                        default:
                            throw RapidClusterError.unknownJoinError
                    }
                }
            }
            try messagingClient.shutdown(el: serverGroup.next())
            try messagingServer.shutdown()
            try serverGroup.syncShutdownGracefully()
            try clientGroup.syncShutdownGracefully()
            throw RapidClusterError.joinFailed
        }


    }



    private func checkIfRunning() throws {
        if (hasShutdown) {
            throw RapidClusterError.clusterAlreadyShutdown
        }
    }
    enum RapidClusterError: Error {
        case clusterAlreadyShutdown
        case joinError(JoinResponse)
        case joinFailed
        case unknownJoinError
    }

    /// ~~~ Events
    enum ClusterEvent {
        case viewChangeProposal([Endpoint])
        case viewChange(ViewChange)
        case kicked
    }

    struct ViewChange {
        let configurationId: UInt64
        let statusChanges: [NodeStatusChange]
    }

    struct NodeStatusChange {
        let node: Endpoint
        let status: EdgeStatus
        let metadata: Metadata
    }

}
