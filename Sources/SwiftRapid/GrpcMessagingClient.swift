import Foundation
import NIO
import NIOConcurrencyHelpers
import GRPC

/// TODO retries
/// TODO error handling for failing connections? / channels - remove them from the clients dict
class GrpcMessagingClient: MessagingClient {

    private let settings: Settings
    internal let group: MultiThreadedEventLoopGroup

    private let clientLock = Lock()
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

        let client: MembershipServiceClient = clientLock.withLock {
            clients[recipient] ?? connect()
        }

        // TODO retry
        return client.sendRequest(msg, callOptions: CallOptions(timeout: timeoutForMessage(msg))).response
    }

    func shutdown(el: EventLoop) throws {
        let terminations = clients.map { (_, client) in
            client.channel.close()
        }
        try _ = EventLoopFuture.whenAllComplete(terminations, on: el).wait()
    }

    private func timeoutForMessage(_ msg: RapidRequest) -> GRPCTimeout {
        func toGRPCTimeout(_ amount: TimeAmount) -> GRPCTimeout {
            try! GRPCTimeout.milliseconds(Int(amount.nanoseconds / 1000000))
        }
        switch msg.content {
            case .joinMessage:
                return toGRPCTimeout(settings.messagingClientJoinRequestTimeout)
            case .probeMessage:
                return toGRPCTimeout(settings.messagingClientProbeRequestTimeout)
            default:
                return toGRPCTimeout(settings.messagingClientDefaultRequestTimeout)
            }
    }
}
