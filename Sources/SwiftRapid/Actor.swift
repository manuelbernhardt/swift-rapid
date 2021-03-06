import Foundation
import NIO
import NIOConcurrencyHelpers

/// Conforming to this protocol adds the ability to schedule tasks to be executed on a "fire and forget"
/// basis and at the same time ensure that only one task is executed at the same time. As a result, only
/// one thread will be accessing the underlying resources at the same time, which is useful for interacting
/// with constructs that are not thread-safe.
///
/// Alternatively to "fire-and-forget", the `ask` method can be used in order to interact with an actor and
/// get an eventual response wrapped in an `EventLoopFuture`
///
/// TODO right now working with this is clunky because the initialized actor doesn't know its own reference so implementations of this protocol have to cater for this in one way or another
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

    private let isStopping: NIOAtomic = NIOAtomic.makeAtomic(value: false)

    init(for actor: A) {
        self.actor = actor
        self.dispatchQueue = DispatchQueue(label: String(describing: actor.self))
    }

    /// Handle a message in a fire-and-forget fashion
    func tell(_ msg: A.MessageType) {
        if (!isStopping.load()) {
            dispatchQueue.async {
                self.actor.receive(msg, nil)
            }
        }
    }

    /// Handle a message that expects to return a response at some point
    func ask(_ msg: A.MessageType) -> EventLoopFuture<A.ResponseType> {
        let promise = actor.el.makePromise(of: A.ResponseType.self)
        if (!isStopping.load()) {
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
        } else {
            promise.fail(ActorError.stopping)
            return promise.futureResult
        }
    }

    /// Starts the actor
    func start() throws {
        try actor.start(ref: self)
    }

    /// Stops the actor
    func stop(el: EventLoop) -> EventLoopFuture<Void> {
        isStopping.store(true)
        return self.actor.stop(el: el)
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

enum ActorError: Error, Equatable {
    case stopping
}
