import Foundation
import NIO

protocol MembershipService {

    func handleRequest(request: RapidRequest) -> EventLoopFuture<RapidResponse>

    func getMemberList() throws -> [Endpoint]

    func getMetadata() throws -> [Endpoint: Metadata]

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
        stateMachineRef.ask(RapidStateMachine.RapidProtocol.rapidRequest(request))
    }

    func getMemberList() throws -> [Endpoint] {
        try self.stateMachine.getMemberList()
    }

    func getMetadata() throws -> [Endpoint: Metadata] {
        try self.stateMachine.getMetadata()
    }

    @discardableResult
    func shutdown() -> EventLoopFuture<()> {
        return self.stateMachine.shutdown()
    }

    deinit {
        shutdown()
    }
}