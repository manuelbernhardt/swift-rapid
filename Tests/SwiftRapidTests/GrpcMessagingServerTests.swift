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
        let server = GrpcMessagingServer(group: group)
        let request = RapidRequest()
        let testClient = TestGrpcMessagingClient(group)

        XCTAss

        try! testClient.sendMessage(recipient: addressFromParts(hostname: "localhost", port: 8000), msg: request).wait()


    }
}

class TestGrpcMessagingClient: GrpcMessagingClient {

}
