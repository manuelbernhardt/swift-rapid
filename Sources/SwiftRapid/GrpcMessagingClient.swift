import Foundation
import NIO
import GRPC

///
/// TODO error handling for failing connections? / channels - remove them from the clients dict
class GrpcMessagingClient: MessagingClient {

    private let settings: Settings
    private let group: MultiThreadedEventLoopGroup

    private var clients = [Endpoint: MembershipServiceClient]()

    init(group: MultiThreadedEventLoopGroup, settings: Settings) {
        self.group = group
        self.settings = settings
    }

    func sendMessage(recipient: Endpoint, msg: RapidRequest) -> EventLoopFuture<RapidResponse> {
        return sendMessage(recipient: recipient, msg: msg, retries: 3) // TODO use settings
    }

    func sendMessageBestEffort(recipient: Endpoint, msg: RapidRequest) -> EventLoopFuture<RapidResponse> {
        return sendMessage(recipient: recipient, msg: msg, retries: 0)
    }

    private func sendMessage(recipient: Endpoint, msg: RapidRequest, retries: Int) -> EventLoopFuture<RapidResponse> {

        func connect() -> MembershipServiceClient {
            let channel = ClientConnection
                    .insecure(group: group)
                    .connect(host: String(decoding: recipient.hostname, as: UTF8.self), port: Int(recipient.port))
            let client = MembershipServiceClient(channel: channel)
            clients[recipient] = client
            return client
        }

        let client: MembershipServiceClient = clients[recipient] ?? connect()
        // TODO retry after timeout
        let timeout = settings.messagingClientRequestTimeout.nanoseconds / 1000000
        return client.sendRequest(msg, callOptions: CallOptions(timeout: try! .milliseconds(Int(timeout)))).response
    }

    func shutdown(el: EventLoop) throws {
        let terminations = clients.map { (_, client) in
            client.channel.close()
        }
        try _ = EventLoopFuture.whenAllComplete(terminations, on: el).wait()
    }
}
