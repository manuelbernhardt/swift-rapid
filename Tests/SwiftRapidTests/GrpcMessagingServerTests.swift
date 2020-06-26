import XCTest
import NIO
@testable import SwiftRapid

class GrpcMessagingServerTest: XCTestCase {

    var group: MultiThreadedEventLoopGroup? = nil

    override func setUp() {
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    }

    override func tearDown() {
        try! group?.syncShutdownGracefully()
    }

    func testHandleMessageWithoutMembershipService() throws {
        let address = addressFromParts("localhost", 8000)
        withServer(address, { server in
            withTestClient { testClient in
                let request = RapidRequest()
                let response: EventLoopFuture<RapidResponse> = testClient.sendMessage(recipient: address, msg: request)
                let _ = try! response.wait()
            }
        })
    }

    func testHandleMessageWithMembershipService() throws {

        let address = addressFromParts("localhost", 8000)
        let testService = TestMembershipService(el: group!.next())
        withServer(address, { server in
            withTestClient { testClient in
                server.onMembershipServiceInitialized(membershipService: testService)
                let request = RapidRequest()
                let response: EventLoopFuture<RapidResponse> = testClient.sendMessage(recipient: address, msg: request)
                let _ = try! response.wait()
            }
        })
    }

    private func withServer<T>(_ address: Endpoint, _ body: (MessagingServer) -> T) -> T {
        let server = GrpcMessagingServer(address: address, group: group!)
        try! server.start()
        defer {
            try! server.shutdown()
        }
        return body(server)
    }

    private func withTestClient<T>(_ body: (MessagingClient) -> T) -> T {
        let testClient = TestGrpcMessagingClient(group: group!, settings: Settings())
        defer {
            try! testClient.shutdown(el: group!.next())
        }
        return body(testClient)
    }

    static var allTests = [
        ("testHandleMessageWithoutMembershipService", testHandleMessageWithoutMembershipService),
        ("testHandleMessageWithMembershipService", testHandleMessageWithMembershipService)
    ]
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
