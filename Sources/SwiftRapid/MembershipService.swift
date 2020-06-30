import Foundation
import NIO

protocol MembershipService {

    func handleRequest(request: RapidRequest) -> EventLoopFuture<RapidResponse>

}

class RapidMembershipService: MembershipService, SerialMessageProcessor {
    typealias MessageType = RapidRequest
    typealias ResponseType = RapidResponse
    
    internal let el: EventLoop
    internal let dispatchQueue = DispatchQueue(label: "rapid.serial.queue")

    private var stateMachine: RapidStateMachine

    /// Initializer for a new cluster (this is the bootstrapping node)
    init(el: EventLoop, selfEndpoint: Endpoint, settings: Settings, failureDetectorProvider: EdgeFailureDetectorProvider,
         broadcaster: Broadcaster, messagingClient: MessagingClient, selfMetadata: Metadata) {
        self.el = el
        self.stateMachine = RapidStateMachine(
            selfEndpoint: selfEndpoint,
            settings: settings,
            failureDetectorProvider: failureDetectorProvider,
            broadcaster: broadcaster,
            messagingClient: messagingClient,
            selfMetadata: selfMetadata
        )
    }
    
    func handleRequest(request: RapidRequest) -> EventLoopFuture<RapidResponse> {
        ask(request)
    }

    func receive(_ msg: MessageType, _ processingCallback: ((ResponseType) -> ())? = nil) {
        do {
            switch msg.content {
                case .joinMessage(let join):
                    let response = try stateMachine.handleJoin(msg: join)
                    processingCallback?(response)
                case .batchedAlertMessage(let alert):
                    let response = try stateMachine.handleAlert(msg: alert)
                    processingCallback?(response)
                case .probeMessage(let probe):
                    let response = try stateMachine.handleProbe(msg: probe)
                    processingCallback?(response)
                case .fastRoundPhase2BMessage, .phase1AMessage, .phase1BMessage, .phase2AMessage, .phase2BMessage:
                    let response = try stateMachine.handleConsensus(msg: msg)
                    processingCallback?(response)
                case .leaveMessage(let leave):
                    let response = try stateMachine.handleLeave(msg: leave)
                    processingCallback?(response)
                case .none:
                    return
            }
        } catch {
            // TODO error handling - need to change the callback so it may fail
        }
    }
}

/// Conforming to this protocol adds the ability to schedule tasks to be executed on a "fire and forget"
/// basis and at the same time ensure that only one task is executed at the same time. As a result, only
/// one thread will be accessing the underlying resources, which is useful for interacting with constructs
/// that are not thread-safe.
protocol SerialMessageProcessor {
    associatedtype MessageType
    associatedtype ResponseType

    var el: EventLoop { get }
    var dispatchQueue: DispatchQueue { get }

    /// The entry point to implement in order to process messages
    ///
    /// - Parameters:
    ///   - msg: the message to process
    ///   - processingCallback: an optional callback passed by the caller to be invoked once processing is done.
    ///                         Can be used both as a way to execute side-effecting code or to return a response
    func receive(_ msg: MessageType, _ processingCallback: ((ResponseType) ->())?)
}
extension SerialMessageProcessor {

    /// Handle a message in a fire-and-forget fashion
    func tell(_ msg: MessageType) {
        dispatchQueue.async {
            self.receive(msg, nil)
        }
    }

    /// Handle a message that expects to return a response at some point
    func ask(_ msg: MessageType) -> EventLoopFuture<ResponseType> {
        let promise = el.makePromise(of: ResponseType.self)
        let callback = { (result: ResponseType) in promise.succeed(result) }
        dispatchQueue.async {
            self.receive(msg, callback)
        }
        return promise.futureResult
    }
}