import Foundation
import NIO

/// Conforming to this protocol adds the ability to schedule tasks to be executed on a "fire and forget"
/// basis and at the same time ensure that only one task is executed at the same time. As a result, only
/// one thread will be accessing the underlying resources at the same time, which is useful for interacting
/// with constructs that are not thread-safe.
///
/// TODO think of a (simple) mechanism by which to simplify the receive (the callback doesn't look all that nice)
protocol Actor {
    associatedtype MessageType
    associatedtype ResponseType

    var el: EventLoop { get }
    var dispatchQueue: DispatchQueue { get }

    /// The entry point to implement in order to process messages
    ///
    /// - Parameters:
    ///   - msg: the message to process
    ///   - callback: an optional callback passed by the caller to be invoked once processing is done.
    ///               Can be used both as a way to execute side-effecting code or to return a response
    mutating func receive(_ msg: MessageType, _ callback: ((ResponseType) ->())?)
}

class ActorRef<A: Actor> {

    private var actor: A

    init(for actor: A) {
        self.actor = actor
    }

    /// Handle a message in a fire-and-forget fashion
    func tell(_ msg: A.MessageType) {
        actor.dispatchQueue.async {
            self.actor.receive(msg, nil)
        }
    }

    /// Handle a message that expects to return a response at some point
    func ask(_ msg: A.MessageType) -> EventLoopFuture<A.ResponseType> {
        let promise = actor.el.makePromise(of: A.ResponseType.self)
        let callback = { (result: A.ResponseType) in promise.succeed(result) }
        actor.dispatchQueue.async {
            self.actor.receive(msg, callback)
        }
        return promise.futureResult
    }

}

class ActorRefProvider {
    private let el: EventLoop

    init(el: EventLoop) {
        self.el = el
    }

    func actorFor<A>(_ actor: A) -> ActorRef<A> where A: Actor {
        ActorRef(for: actor)
    }
}