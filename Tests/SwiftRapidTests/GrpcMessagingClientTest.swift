import XCTest
import NIO
import NIOConcurrencyHelpers
import GRPC

@testable import SwiftRapid


class GrpcMessagingClientTest: XCTestCase {

    var serverGroup: MultiThreadedEventLoopGroup? = nil
    var clientGroup: MultiThreadedEventLoopGroup? = nil
    var clientSettings: Settings = Settings()

    override func setUp() {
        serverGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        clientGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        clientSettings = Settings()
    }

    override func tearDown() {
        try! clientGroup?.syncShutdownGracefully()
        try! serverGroup?.syncShutdownGracefully()
    }

    func testSuccessfulClientCall() throws {
        let address = addressFromParts("localhost", 8000)
        withTestServer(address, { server in
            withClient { client in
                let request = RapidRequest()
                let response = client.sendMessage(recipient: address, msg: request)
                let _ = try! response.wait()
                XCTAssertEqual(1, server.requestCount())
            }
        })
    }

    func testTimingOutClientCall() throws {
        let address = addressFromParts("localhost", 8000)
        clientSettings.MessagingClientRequestTimeoutInMs = 100
        withTestServer(address, { server in
            withClient { client in
                let request = RapidRequest()
                server.responseDelayInSeconds = 1
                let response = client.sendMessage(recipient: address, msg: request)
                do {
                    let _ = try response.wait()
                    XCTFail()
                } catch {
                    // okay
                }
            }
        })

    }

    private func withTestServer<T>(_ address: Endpoint, _ body: (TestMessagingServer) -> T) -> T {
        let server = TestMessagingServer(address: address, group: serverGroup!)
        try! server.start()
        defer {
            try! server.shutdown()
        }
        return body(server)
    }

    private func withClient<T>(_ body: (MessagingClient) -> T) -> T {
        let client = GrpcMessagingClient(group: clientGroup!, settings: clientSettings)
        defer {
            try! client.shutdown(el: clientGroup!.next())
        }
        return body(client)
    }

    static var allTests = [
        ("testSuccessfulClientCall", testSuccessfulClientCall),
        ("testTimingOutClientCall", testTimingOutClientCall)
    ]

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
