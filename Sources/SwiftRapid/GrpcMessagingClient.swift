import Foundation
import NIO
import NIOConcurrencyHelpers
import GRPC

/// TODO error handling for failing connections? / channels - remove them from the clients dict
class GrpcMessagingClient: MessagingClient {

    private let isShuttingDown = NIOAtomic.makeAtomic(value: false)

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

    private func sendMessage(recipient: Endpoint, msg: RapidRequest, retries: Int, attempt: Int = 0) -> EventLoopFuture<RapidResponse> {
        func connect() -> MembershipServiceClient {
            let channel = ClientConnection
                    .insecure(group: group)
                    .connect(host: String(decoding: recipient.hostname, as: UTF8.self), port: Int(recipient.port))
            let client = MembershipServiceClient(channel: channel)
            clients[recipient] = client
            return client
        }

        if (!isShuttingDown.load()) {

            let client: MembershipServiceClient = clientLock.withLock {
                clients[recipient] ?? connect()
            }

            return client.sendRequest(msg, callOptions: CallOptions(timeout: timeoutForMessage(msg)))
                    .response
                    .flatMapError({ (error: Error) in
                let loop = self.group.next()
                let failed: EventLoopFuture<RapidResponse> = loop.makeFailedFuture(error)
                if (attempt < retries) {
                    return self.sendMessage(recipient: recipient, msg: msg, retries: retries, attempt: attempt + 1)
                } else {
                    return failed
                }
            })

        } else {
            let failed: EventLoopFuture<RapidResponse> = self.group.next().makeFailedFuture(GrpcMessagingError.shutdownInProgress)
            return failed
        }

    }

    func shutdown(el: EventLoop) -> EventLoopFuture<Void> {
        isShuttingDown.store(true)
        let terminations = clients.map { (_, client) in
            client.channel.close().hop(to: el)
        }
        return EventLoopFuture.whenAllComplete(terminations, on: el).map { _ in
            ()
        }
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

enum GrpcMessagingError: Error {
    case shutdownInProgress
}