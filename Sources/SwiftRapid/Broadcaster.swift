import Foundation
import NIO

/// A broadcaster responsible for relaying messages to all nodes specified by the setMembership method
protocol Broadcaster {

    @discardableResult
    func broadcast(request: RapidRequest) -> EventLoopFuture<[Result<RapidResponse, Error>]>

    func setMembership(recipients: [Endpoint])
}

class UnicastToAllBroadcaster: Broadcaster {
    private let el: EventLoop
    private let client: MessagingClient
    private var recipients = [Endpoint]()

    init(client: MessagingClient, el: EventLoop) {
        self.el = el
        self.client = client
    }

    func broadcast(request: RapidRequest) -> EventLoopFuture<[Result<RapidResponse, Error>]> {
        EventLoopFuture.whenAllComplete(recipients.map { recipient in
            client.sendMessageBestEffort(recipient: recipient, msg: request)
        }, on: el)
    }

    func setMembership(recipients: [Endpoint]) {
        self.recipients = recipients
    }
}
