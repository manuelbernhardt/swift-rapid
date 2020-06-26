import XCTest
import NIO
@testable import SwiftRapid

class GrpcMessagingServerTest: XCTestCase, TestClientMessaging {

    var clientGroup: MultiThreadedEventLoopGroup? = nil
    var clientSettings: Settings = Settings()

    override func setUp() {
        clientGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        clientSettings = Settings()
    }

    override func tearDown() {
        try! clientGroup?.syncShutdownGracefully()
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
        let testService = TestMembershipService(el: clientGroup!.next())
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
        let server = GrpcMessagingServer(address: address, group: clientGroup!)
        try! server.start()
        defer {
            try! server.shutdown()
        }
        return body(server)
    }

    static var allTests = [
        ("testHandleMessageWithoutMembershipService", testHandleMessageWithoutMembershipService),
        ("testHandleMessageWithMembershipService", testHandleMessageWithMembershipService)
    ]
}