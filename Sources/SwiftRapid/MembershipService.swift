import Foundation
import NIO

protocol MembershipService {

    func handleRequest(request: RapidRequest) -> EventLoopFuture<RapidResponse>

}

class RapidMembershipService: MembershipService {

    private let el: EventLoop
    private let stateMachine: ActorRef<RapidStateMachine>

    /// Initializer for a new cluster (this is the bootstrapping node)
    init(el: EventLoop, selfEndpoint: Endpoint, settings: Settings, failureDetectorProvider: EdgeFailureDetectorProvider,
         broadcaster: Broadcaster, messagingClient: MessagingClient, selfMetadata: Metadata) throws {

        self.el = el

        // TODO don't build this here
        let provider = ActorRefProvider(el: el)

        let stateMachine = try RapidStateMachine(
                selfEndpoint: selfEndpoint,
                settings: settings,
                failureDetectorProvider: failureDetectorProvider,
                broadcaster: broadcaster,
                messagingClient: messagingClient,
                selfMetadata: selfMetadata,
                el: el
        )
        self.stateMachine = provider.actorFor(stateMachine)
        try stateMachine.start(ref: self.stateMachine)
    }
    
    func handleRequest(request: RapidRequest) -> EventLoopFuture<RapidResponse> {
        stateMachine.ask(RapidStateMachine.RapidProtocol.rapidRequest(request))
    }

}