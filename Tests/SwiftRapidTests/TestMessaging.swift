import NIO
import NIOConcurrencyHelpers
import GRPC
@testable import SwiftRapid

protocol TestServerMessaging {
    var serverGroup: MultiThreadedEventLoopGroup? { get }
}

protocol TestClientMessaging {
    var clientGroup: MultiThreadedEventLoopGroup? { get }
    var clientSettings: Settings { get }
}

extension TestServerMessaging {
    func withTestServer<T>(_ address: Endpoint, _ body: (TestMessagingServer) -> T) -> T {
        let server = TestMessagingServer(address: address, group: serverGroup!)
        try! server.start()
        defer {
            try! server.shutdown()
        }
        return body(server)
    }
}

extension TestClientMessaging {
    func withTestClient<T>(_ body: (MessagingClient) -> T) -> T {
        let testClient = TestGrpcMessagingClient(group: clientGroup!, settings: clientSettings)
        defer {
            try! testClient.shutdown(el: clientGroup!.next())
        }
        return body(testClient)
    }

}


class TestMessagingServer: GrpcMessagingServer {

    private let group: MultiThreadedEventLoopGroup
    private var requests: [RapidRequest] = []
    private let lock: Lock = Lock()

    var responseDelayInSeconds: UInt32 = 0

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
}

