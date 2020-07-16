import Foundation
import NIO

/// Conforming to this protocol adds the ability to schedule tasks to be executed on a "fire and forget"
/// basis and at the same time ensure that only one task is executed at the same time. As a result, only
/// one thread will be accessing the underlying resources at the same time, which is useful for interacting
/// with constructs that are not thread-safe.
///
/// TODO right now working with this is clunky because the initialized actor doesn't know its own reference so implementations
/// TODO of this protocol have to cater for this in one way or another
///
/// TODO the actor ref provider should ideally manage the event loops for all actors - maybe. in truth these EL's don't do much,
/// TODO they are used by the actor ref's ask to provide a promise
///
/// TODO think of a (simple) mechanism by which to simplify the receive (the callback doesn't look all that nice)
protocol Actor {
    associatedtype MessageType
    associatedtype ResponseType

    var el: EventLoop { get }

    /// The entry point to implement in order to process messages
    ///
    /// - Parameters:
    ///   - msg: the message to process
    ///   - callback: an optional callback passed by the caller to be invoked once processing is done.
    ///               Can be used both as a way to execute side-effecting code or to return a response
    mutating func receive(_ msg: MessageType, _ callback: ((Result<ResponseType, Error>) ->())?)

    /// Perform any startup tasks here, including storing the ActorRef if needed
    ///
    /// - Parameter ref: the ActorRef of this actor
    /// TODO it would be nice if we didn't have to do this explicitly, but I don't see how.
    func start(ref: ActorRef<Self>) throws

    /// Perform any shutdown tasks here
    func stop(el: EventLoop) -> EventLoopFuture<Void>
}

class ActorRef<A: Actor> {

    private var actor: A

    private let dispatchQueue: DispatchQueue

    init(for actor: A) {
        self.actor = actor
        self.dispatchQueue = DispatchQueue(label: String(describing: actor.self))
    }

    /// Handle a message in a fire-and-forget fashion
    func tell(_ msg: A.MessageType) {
        dispatchQueue.async {
            self.actor.receive(msg, nil)
        }
    }

    /// Handle a message that expects to return a response at some point
    func ask(_ msg: A.MessageType) -> EventLoopFuture<A.ResponseType> {
        let promise = actor.el.makePromise(of: A.ResponseType.self)
        let callback = { (result: Result<A.ResponseType, Error>) in
            switch result {
                case .success(let value):
                    promise.succeed(value)
                case .failure(let error):
                    promise.fail(error)
                }
        }
        dispatchQueue.async {
            self.actor.receive(msg, callback)
        }
        return promise.futureResult
    }

    /// Starts the actor
    func start() throws {
        try actor.start(ref: self)
    }

    /// Stops the actor
    func stop(el: EventLoop) -> EventLoopFuture<Void> {
        self.actor.stop(el: el)
    }


}

class ActorRefProvider {
    private let group: MultiThreadedEventLoopGroup

    init(group: MultiThreadedEventLoopGroup) {
        self.group = group
    }

    // TODO try to make this into a builder
    func actorFor<A>(_ creator: (EventLoop) throws -> A) rethrows -> ActorRef<A> where A: Actor {
        ActorRef(for: try creator(group.next()))
    }

}