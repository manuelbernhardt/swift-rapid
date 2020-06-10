import Foundation
import NIO

/// A broadcaster responsible for relaying messages to all nodes specified by the setMembership method
protocol Broadcaster {

    @discardableResult
    func broadcast(request: RapidRequest) -> [EventLoopFuture<RapidResponse>]

    func setMembership(recipients: [Endpoint])

}
