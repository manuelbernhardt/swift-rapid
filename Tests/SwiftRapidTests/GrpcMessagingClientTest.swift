import XCTest
import NIO
import NIOConcurrencyHelpers
import GRPC

@testable import SwiftRapid


class GrpcMessagingClientTest: XCTestCase, TestClientMessaging, TestServerMessaging {

    var eventLoopGroup: MultiThreadedEventLoopGroup? = nil
    var clientSettings: Settings = Settings()

    override func setUp() {
        eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 4)
        clientSettings = Settings()
    }

    override func tearDown() {
        try! eventLoopGroup?.syncShutdownGracefully()
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
        clientSettings.messagingClientDefaultRequestTimeout = TimeAmount.milliseconds(100)
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

    private func withClient<T>(_ body: (MessagingClient) -> T) -> T {
        let client = GrpcMessagingClient(group: eventLoopGroup!, settings: clientSettings)
        defer {
            try! client.shutdown(el: eventLoopGroup!.next())
        }
        return body(client)
    }

    static var allTests = [
        ("testSuccessfulClientCall", testSuccessfulClientCall),
        ("testTimingOutClientCall", testTimingOutClientCall)
    ]

}

