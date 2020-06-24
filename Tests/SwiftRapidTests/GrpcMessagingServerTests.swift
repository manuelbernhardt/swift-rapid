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

    func testHandleMessageWithoutService() throws {
        let address = addressFromParts("localhost", 8000)
        if let g = group {
            _ = GrpcMessagingServer(address: address, group: g)
            let request = RapidRequest()
            let testClient = TestGrpcMessagingClient(group: g, settings: Settings())

            XCTAssertNoThrow({
                let response: EventLoopFuture<RapidResponse> = try testClient.sendMessage(recipient: address, msg: request)
                try response.wait()
            })
        }
    }

    static var allTests = [
        ("testHandleMessageWithoutService", testHandleMessageWithoutService)
    ]
}

class TestGrpcMessagingClient: GrpcMessagingClient {

}
