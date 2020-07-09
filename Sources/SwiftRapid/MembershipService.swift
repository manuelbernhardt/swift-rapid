import Foundation
import NIO

protocol MembershipService {

    func handleRequest(request: RapidRequest) -> EventLoopFuture<RapidResponse>

}

class RapidMembershipService: MembershipService {

    private let provider: ActorRefProvider
    private let stateMachine: RapidStateMachine
    private let stateMachineRef: ActorRef<RapidStateMachine>

    /// Initializer for a new cluster (this is the bootstrapping node)
    init(selfEndpoint: Endpoint, settings: Settings, view: MembershipView, failureDetectorProvider: EdgeFailureDetectorProvider,
         broadcaster: Broadcaster, messagingClient: MessagingClient, selfMetadata: Metadata,
         provider: ActorRefProvider, el: EventLoop) throws {

        self.provider = provider

        let stateMachine = try RapidStateMachine(
                selfEndpoint: selfEndpoint,
                settings: settings,
                view: view,
                failureDetectorProvider: failureDetectorProvider,
                broadcaster: broadcaster,
                messagingClient: messagingClient,
                selfMetadata: selfMetadata,
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

    @discardableResult
    func shutdown() -> EventLoopFuture<()> {
        return self.stateMachine.shutdown()
    }

    deinit {
        shutdown()
    }
}