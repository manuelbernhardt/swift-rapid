import Foundation
import NIO
import NIOConcurrencyHelpers

/// The API of the Rapid membership service
public protocol MembershipService {

    func handleRequest(request: RapidRequest) -> EventLoopFuture<RapidResponse>

    func getMemberList() throws -> EventLoopFuture<[Endpoint]>

    func getMetadata() throws -> EventLoopFuture<[Endpoint: Metadata]>

    func shutdown(el: EventLoop) -> EventLoopFuture<Void>
}

class RapidMembershipService: MembershipService {

    private let provider: ActorRefProvider
    private let stateMachineRef: ActorRef<RapidStateMachine>
    private let el: EventLoop
    private let isShutdown = NIOAtomic.makeAtomic(value: false)

    /// Initializes the membership service
    init(selfEndpoint: Endpoint, settings: Settings, view: MembershipView, failureDetectorProvider: EdgeFailureDetectorProvider,
         broadcaster: Broadcaster, messagingClient: MessagingClient, allMetadata: [Endpoint: Metadata],
         subscriptions: [(RapidCluster.ClusterEvent) -> ()],
         provider: ActorRefProvider, el: EventLoop) throws {

        self.provider = provider
        self.el = el

        let ref = try provider.actorFor { el in
            try RapidStateMachine(
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
        }
        self.stateMachineRef = ref
        try self.stateMachineRef.start()
    }

    func handleRequest(request: RapidRequest) -> EventLoopFuture<RapidResponse> {
        if (isShutdown.load()) {
            // TODO think this over
            return el.makeSucceededFuture(RapidResponse())
        } else {
            return stateMachineRef.ask(RapidStateMachine.RapidCommand.rapidRequest(request)).map { result in
                switch result {
                case .rapidResponse(let response):
                    return response
                default:
                    fatalError("Should not be here")
                }
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

    func shutdown(el: EventLoop) -> EventLoopFuture<Void> {
        self.isShutdown.store(true)
        return stateMachineRef.stop(el: el)
    }
}