import Foundation
import NIO

/// A broadcaster responsible for relaying messages to all nodes specified by the setMembership method
protocol Broadcaster {

    @discardableResult
    func broadcast(request: RapidRequest) -> [EventLoopFuture<RapidResponse>]

    func setMembership(recipients: [Endpoint])
}

class UnicastToAllBroadcaster: Broadcaster {
    private let client: MessagingClient
    private var recipients = [Endpoint]()

    init(client: MessagingClient) {
        self.client = client
    }

    func broadcast(request: RapidRequest) -> [EventLoopFuture<RapidResponse>] {
        recipients.map { recipient in
            client.sendMessageBestEffort(recipient: recipient, msg: request)
        }
    }

    func setMembership(recipients: [Endpoint]) {
        self.recipients = recipients
    }
}
