import Foundation
import NIO

public protocol MembershipService {

    func handleRequest(request: RapidRequest) -> EventLoopFuture<RapidResponse>

    func getMemberList() throws -> EventLoopFuture<[Endpoint]>

    func getMetadata() throws -> EventLoopFuture<[Endpoint: Metadata]>

    func shutdown() throws -> EventLoopFuture<()>
}

class RapidMembershipService: MembershipService {

    private let provider: ActorRefProvider
    private let stateMachine: RapidStateMachine
    private let stateMachineRef: ActorRef<RapidStateMachine>

    /// Initializes the membership service
    init(selfEndpoint: Endpoint, settings: Settings, view: MembershipView, failureDetectorProvider: EdgeFailureDetectorProvider,
         broadcaster: Broadcaster, messagingClient: MessagingClient, allMetadata: [Endpoint: Metadata],
         subscriptions: [(RapidCluster.ClusterEvent) -> ()],
         provider: ActorRefProvider, el: EventLoop) throws {
        self.provider = provider

        let stateMachine = try RapidStateMachine(
                selfEndpoint: selfEndpoint,
                settings: settings,
                view: view,
                failureDetectorProvider: failureDetectorProvider,
                broadcaster: broadcaster,
                messagingClient: messagingClient,
                allMetadata: allMetadata,
                subscriptions: subscriptions,
                el: el
        )
        let ref = provider.actorFor(stateMachine)
        self.stateMachine = stateMachine
        self.stateMachineRef = ref
        try stateMachine.start(ref: self.stateMachineRef)
    }

    func handleRequest(request: RapidRequest) -> EventLoopFuture<RapidResponse> {
        stateMachineRef.ask(RapidStateMachine.RapidCommand.rapidRequest(request)).map { result in
            switch result {
            case .rapidResponse(let response):
                return response
            default:
                fatalError("Should not be here")
            }
        }
    }

    func getMemberList() -> EventLoopFuture<[Endpoint]> {
        stateMachineRef.ask(RapidStateMachine.RapidCommand.retrieveMemberList).map { result in
            switch result {
            case .memberList(let list):
                return list
            default:
                return []
            }
        }
    }

    func getMetadata() throws -> EventLoopFuture<[Endpoint: Metadata]> {
        stateMachineRef.ask(RapidStateMachine.RapidCommand.retrieveMetadata).map { result in
            switch result {
            case .metadata(let metadata):
                return metadata
            default:
                return [:]
            }
        }
    }

    @discardableResult
    func shutdown() -> EventLoopFuture<()> {
        self.stateMachine.shutdown()
    }

    deinit {
        shutdown()
    }
}