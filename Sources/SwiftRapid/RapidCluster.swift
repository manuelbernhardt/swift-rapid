import Foundation
import NIO
import Logging

/// The entry point for using SwiftRapid
/// It uses a "builder" pattern for obtaining new instances, like so:
///
/// try RapidCluster.Builder.with {
///     $0.host = "localhost"
///     $0.port = 8000
/// }.start()
///
/// or
///
/// try RapidCluster.Builder.with {
///     $0.host = "localhost"
///     $0.port = 8000
/// }.join(host: "localhost", port: 8000)
///
///
/// TODO leaving
/// TODO listener for shutdown
final public class RapidCluster {
    private let membershipService: MembershipService
    private let messagingServer: MessagingServer
    private let messagingClient: MessagingClient
    private let eventLoopGroup: MultiThreadedEventLoopGroup
    private let listenAddress: Endpoint
    private var hasShutdown = false

    private init(messagingServer: MessagingServer,
                 messagingClient: MessagingClient,
                 membershipService: MembershipService,
                 listenAddress: Endpoint,
                 eventLoopGroup: MultiThreadedEventLoopGroup) {
        self.membershipService = membershipService
        self.messagingServer = messagingServer
        self.messagingClient = messagingClient
        self.eventLoopGroup = eventLoopGroup
        self.listenAddress = listenAddress
    }

    ///
    /// Retrieves the endpoint of this cluster
    public func getEndpoint() -> Endpoint {
        self.listenAddress
    }

    ///
    /// Retrieves the member list
    public func getMemberList() throws -> [Endpoint] {
        try checkIfRunning()
        return try membershipService.getMemberList().wait()
    }

    ///
    /// Retrieves the members and their metadata
    public func getClusterMetadata() throws -> [Endpoint: Metadata] {
        try checkIfRunning()
        return try membershipService.getMetadata().wait()
    }

    ///
    /// Leaves the cluster gracefully by communicating intent rather than just leaving and letting the failure detectors take care of it
    public func leaveGracefully() throws {
        fatalError("Not implemented")
    }

    ///
    /// Shuts the cluster down
    public func shutdown() throws {
        // hmm...
        // TODO this is evidently not the cleanest approach and it is actually buggy
        // shutting down the event loop at the end may happen too soon in some cases, at least we've got a leaking loop
        // somewhere. unsure as to what causes it though
        // in any case we want to maintain the semantics of shutting down other components (client and server) independently of the membership service
        // shutdown succeeding or not
        // order matters as well for a graceful shutdown - without client / server, the membership service can't work and we get leaking promises
        let shutdownLoop = eventLoopGroup.next()
        membershipService.shutdown(el: shutdownLoop).whenComplete { _ in
            self.messagingServer.shutdown(el: shutdownLoop).whenComplete { _ in
                self.messagingClient.shutdown(el: shutdownLoop).whenComplete { _ in
                    self.eventLoopGroup.shutdownGracefully { _ in
                        ()
                    }
                }
            }
        }
    }

    public struct Builder {
        private let logger = Logger(label: "rapid.RapidCluster")

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

        public mutating func registerSubscription(callback: @escaping (ClusterEvent) -> ()) {
            eventSubscriptions.append(callback)
        }

        public func start() throws -> RapidCluster {
            precondition(host != "", "host is not set")
            precondition(port != 0, "port is not set")
            let selfEndpoint = addressFromParts(host, port)
            // TODO configurable
            let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 4)
            let messagingServer: MessagingServer = self.messagingServer ?? GrpcMessagingServer(address: selfEndpoint, group: eventLoopGroup)
            let messagingClient: MessagingClient = self.messagingClient ?? GrpcMessagingClient(group: eventLoopGroup, settings: settings)
            let broadcaster = UnicastToAllBroadcaster(client: messagingClient, el: eventLoopGroup.next())
            let currentIdentifier = nodeIdFromUUID(UUID())
            // TODO should also be assigned a group and be the one to hand out event loops
            let actorRefProvider = ActorRefProvider(group: eventLoopGroup)
            let edgeFailureDetectorProvider = self.edgeFailureDetectorProvider ?? AdaptiveAccrualFailureDetectorProvider(selfEndpoint: selfEndpoint, messagingClient: messagingClient, provider: actorRefProvider, settings: self.settings, el: eventLoopGroup.next())
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
                    el: eventLoopGroup.next()
            )
            messagingServer.onMembershipServiceInitialized(membershipService: membershipService)
            try messagingServer.start()
            logger.info("Successfully started Rapid cluster")
            return RapidCluster(
                    messagingServer: messagingServer,
                    messagingClient: messagingClient,
                    membershipService: membershipService,
                    listenAddress: selfEndpoint,
                    eventLoopGroup: eventLoopGroup)
        }

        public func join(host: String, port: Int) throws -> RapidCluster {
            try join(seedEndpoint: addressFromParts(host, port))
        }

        func join(seedEndpoint: Endpoint) throws -> RapidCluster {
            precondition(self.host != "", "host is not set")
            precondition(self.port != 0, "port is not set")
            let listenAddress = addressFromParts(self.host, self.port)
            var currentIdentifier = nodeIdFromUUID(UUID())
            // TODO configurable
            let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 4)
            let messagingServer: MessagingServer = self.messagingServer ?? GrpcMessagingServer(address: listenAddress, group: eventLoopGroup)
            let messagingClient: MessagingClient = self.messagingClient ?? GrpcMessagingClient(group: eventLoopGroup, settings: settings)
            let broadcaster = UnicastToAllBroadcaster(client: messagingClient, el: eventLoopGroup.next())
            // TODO should also be assigned a group and be the one to hand out event loops
            let actorRefProvider = ActorRefProvider(group: eventLoopGroup)
            let edgeFailureDetectorProvider = self.edgeFailureDetectorProvider ?? AdaptiveAccrualFailureDetectorProvider(selfEndpoint: listenAddress, messagingClient: messagingClient, provider: actorRefProvider, settings: self.settings, el: eventLoopGroup.next())

            func joinAttempt(seedEndpoint: Endpoint, listenAddress: Endpoint, nodeId: NodeId, attempt: Int) throws -> RapidCluster {
                let joinRequest = RapidRequest.with {
                    $0.joinMessage = JoinMessage.with {
                        $0.sender = listenAddress
                        $0.nodeID = nodeId
                        $0.metadata = metadata
                    }
                }
                let joinResponse = try messagingClient.sendMessage(recipient: seedEndpoint, msg: joinRequest).wait().joinResponse
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
                        allMetadata: allMetadata,
                        subscriptions: eventSubscriptions,
                        provider: actorRefProvider,
                        el: eventLoopGroup.next()
                )
                messagingServer.onMembershipServiceInitialized(membershipService: membershipService)
                try messagingServer.start()
                logger.info("Successfully joined Rapid cluster with \(response.endpoints.count) members")
                return RapidCluster(
                        messagingServer: messagingServer,
                        messagingClient: messagingClient,
                        membershipService: membershipService,
                        listenAddress: selfEndpoint,
                        eventLoopGroup: eventLoopGroup
                )
            }

            for attempt in 0..<settings.joinAttempts {
                do {
                    return try joinAttempt(seedEndpoint: seedEndpoint, listenAddress: listenAddress, nodeId: currentIdentifier, attempt: attempt)
                } catch RapidClusterError.joinError(let joinResponse) {
                    switch joinResponse.statusCode {
                    case .uuidAlreadyInRing:
                        logger.error("Node with the same UUID already present. Retrying.")
                        currentIdentifier = nodeIdFromUUID(UUID())
                        sleep(UInt32(settings.joinDelaySeconds))
                        break
                    case .hostnameAlreadyInRing:
                        logger.error("Membership rejected, retrying.")
                        sleep(UInt32(settings.joinDelaySeconds))
                        break
                    case .viewChangeInProgress:
                        logger.error("Seed node is executing a view change, retrying.")
                        sleep(UInt32(settings.joinDelaySeconds))
                        break
                    default:
                        throw RapidClusterError.unknownJoinError
                    }
                }
            }
            let shutdownLoop = eventLoopGroup.next()
            try messagingClient.shutdown(el: shutdownLoop).wait()
            try messagingServer.shutdown(el: shutdownLoop).wait()
            // TODO this sometimes appears to cause flakiness in tests
            eventLoopGroup.shutdownGracefully({ _ in () })
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
    public enum ClusterEvent {
        case viewChangeProposal([Endpoint])
        case viewChange(ViewChange)
        case kicked
    }

    public struct ViewChange {
        let configurationId: UInt64
        let statusChanges: [NodeStatusChange]
    }

    public struct NodeStatusChange {
        let node: Endpoint
        let status: EdgeStatus
        let metadata: Metadata
    }

}