import NIO
import NIOConcurrencyHelpers
import GRPC
import Dispatch
@testable import SwiftRapid

protocol TestServerMessaging {
    var serverGroup: MultiThreadedEventLoopGroup? { get }
}

protocol TestClientMessaging {
    var clientGroup: MultiThreadedEventLoopGroup? { get }
    var clientSettings: Settings { get }
}

extension TestServerMessaging {
    func withTestServer<T>(_ address: Endpoint, _ body: (TestMessagingServer) -> T, disableTestMembershipService: Bool = false) -> T {
        let server = TestMessagingServer(address: address, group: serverGroup!, disableTestMembershipService: disableTestMembershipService)
        try! server.start()
        defer {
            try! server.shutdown()
        }
        return body(server)
    }
}

extension TestClientMessaging {
    func withTestClient<T>(_ body: (MessagingClient) -> T, delay: TimeAmount = TimeAmount.nanoseconds(0)) -> T {
        var testClient = TestGrpcMessagingClient(group: clientGroup!, settings: clientSettings)
        testClient.delayBestEffortMessages(for: delay)
        defer {
            if let group = clientGroup {
                try! testClient.shutdown(el: group.next())
            }
        }
        return body(testClient)
    }

}


class TestMessagingServer: GrpcMessagingServer {

    private let group: MultiThreadedEventLoopGroup
    private var requests: [RapidRequest] = []
    private let lock: Lock = Lock()

    var responseDelayInSeconds: UInt32 = 0

    init(address: Endpoint, group: MultiThreadedEventLoopGroup, disableTestMembershipService: Bool) {
        self.group = group
        super.init(address: address, group: group)
    }

    override init(address: Endpoint, group: MultiThreadedEventLoopGroup) {
        self.group = group
        super.init(address: address, group: group)
        let testMembershipService = TestMembershipService(el: group.next())
        onMembershipServiceInitialized(membershipService: testMembershipService)
    }

    func requestCount() -> Int {
        lock.withLock {
            requests.count
        }
    }

    override func sendRequest(request: RapidRequest, context: StatusOnlyCallContext) -> EventLoopFuture<RapidResponse> {

        sleep(responseDelayInSeconds)

        return lock.withLock {
            requests.append(request)
            return super.sendRequest(request: request, context: context)
        }
    }
}

class TestGrpcMessagingClient: GrpcMessagingClient {

    private var delay = TimeAmount.nanoseconds(0)

    func delayBestEffortMessages(for amount: TimeAmount) {
        self.delay = amount
    }

    override func sendMessageBestEffort(recipient: Endpoint, msg: RapidRequest) -> EventLoopFuture<RapidResponse> {
        let el = group.next()
        let promise = el.makePromise(of: RapidResponse.self)
        el.scheduleTask(in: delay, {
            promise.completeWith(super.sendMessageBestEffort(recipient: recipient, msg: msg))
        })
        return promise.futureResult
    }
}

class TestMembershipService: MembershipService {
    let el: EventLoop
    var request: RapidRequest? = nil
    init(el: EventLoop) {
        self.el = el
    }
    func handleRequest(request: RapidRequest) -> EventLoopFuture<RapidResponse> {
        self.request = request
        let response = RapidResponse()
        return el.makeSucceededFuture(response)
    }

    func getMemberList() throws -> [Endpoint] {
        fatalError("getMemberList() has not been implemented")
    }

    func getMetadata() throws -> [Endpoint: Metadata] {
        fatalError("getMetadata() has not been implemented")
    }
}

