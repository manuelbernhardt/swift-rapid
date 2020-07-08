import XCTest
import NIO
@testable import SwiftRapid

class MessagingTest: XCTestCase, TestServerMessaging, TestClientMessaging {

    var clientGroup: MultiThreadedEventLoopGroup? = nil
    var serverGroup: MultiThreadedEventLoopGroup? = nil
    var clientSettings = Settings()

    var settings = Settings()
    var provider = ActorRefProvider()

    override func setUp() {
        serverGroup = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        clientGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        clientSettings = Settings()
        settings = Settings()
    }

    override func tearDown() {
        try! serverGroup?.syncShutdownGracefully()
        try! clientGroup?.syncShutdownGracefully()
    }

    func testJoinFirstNode() throws {
        let serverAddress = addressFromParts("127.0.0.1", 1234)
        let clientAddress = addressFromParts("127.0.0.1", 1235)

        withTestClient { client in
            withTestServer(serverAddress, { server in
                let service = try! createMembershipService(serverAddress: serverAddress, client: client, server: server)
                let response = try! sendJoinMessage(client: client, serverAddress: serverAddress, clientAddress: clientAddress, nodeId: nodeIdFromUUID(UUID()))
                XCTAssertEqual(response.statusCode, JoinStatusCode.safeToJoin)
                XCTAssertEqual(response.endpoints.count, 2)

                try! service.shutdown().wait()
            })
        }
    }

    private func sendJoinMessage(client: MessagingClient, serverAddress: Endpoint, clientAddress: Endpoint, nodeId: NodeId) throws -> JoinResponse {
        let msg = RapidRequest.with {
            $0.joinMessage = JoinMessage.with {
                $0.sender = clientAddress
                $0.nodeID = nodeId
            }
        }
        return try client.sendMessage(recipient: serverAddress, msg: msg).wait().joinResponse
    }

    private func createMembershipService(serverAddress: Endpoint, client: MessagingClient, server: MessagingServer) throws -> RapidMembershipService {
        let view = MembershipView(K: settings.K)
        try view.ringAdd(node: serverAddress, nodeId: nodeIdFromUUID(UUID()))
        let broadcaster = UnicastToAllBroadcaster(client: client)
        let failureDetectorProvider = AdaptiveAccrualFailureDetectorProvider(selfEndpoint: serverAddress, messagingClient: client, provider: provider, el: serverGroup!.next())
        let membershipService = try RapidMembershipService(selfEndpoint: serverAddress, settings: settings, view: view, failureDetectorProvider: failureDetectorProvider,
                broadcaster: broadcaster, messagingClient: client, selfMetadata: Metadata(), provider: provider, el: serverGroup!.next())
        server.onMembershipServiceInitialized(membershipService: membershipService)
        try server.start()
        return membershipService
    }

    static var allTests = [
        ("testJoinFirstNode", testJoinFirstNode),
    ]


}
