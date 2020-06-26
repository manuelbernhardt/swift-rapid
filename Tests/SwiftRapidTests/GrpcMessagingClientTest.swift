import XCTest
import NIO
import NIOConcurrencyHelpers
import GRPC

@testable import SwiftRapid


class GrpcMessagingClientTest: XCTestCase, TestClientMessaging, TestServerMessaging {

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

