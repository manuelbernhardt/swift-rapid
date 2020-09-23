import Foundation
import NIO

/// The client side of the pluggable messaging API
public protocol MessagingClient {

    /// Sends a message to a remote node with redelivery semantics
    /// - Parameters:
    ///   - recipient: the remote node to send the message to
    ///   - msg: the message to send
    /// - Returns: a future that returns a RapidResponse if the call was successful
    ///
   @discardableResult
   func sendMessage(recipient: Endpoint, msg: RapidRequest) -> EventLoopFuture<RapidResponse>

    /// Sends a message to a remote node with best-effort semantics
    /// - Parameters:
    ///   - recipient: the remote node to send the message to
    ///   - msg: the message to send
    /// - Returns: a future that returns a RapidResponse if the call was successful
    @discardableResult
    func sendMessageBestEffort(recipient: Endpoint, msg: RapidRequest) -> EventLoopFuture<RapidResponse>

    /// Signals the messaging client that it should free up resources in use
    func shutdown(el: EventLoop) -> EventLoopFuture<Void>


}
