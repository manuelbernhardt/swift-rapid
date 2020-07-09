import XCTest
import NIO
import NIOConcurrencyHelpers
import Dispatch
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
        let nodeAddress = addressFromParts("127.0.0.1", 1234)
        let joiningNodeAddress = addressFromParts("127.0.0.1", 1235)

        withTestClient { client in
            withTestServer(nodeAddress, { server in
                let service = try! createMembershipService(serverAddress: nodeAddress, client: client, server: server)
                let response = try! sendJoinMessage(client: client, nodeAddress: nodeAddress, joiningNodeAddress: joiningNodeAddress, joiningNodeId: nodeIdFromUUID(UUID()))
                XCTAssertEqual(JoinStatusCode.safeToJoin, response.statusCode)
                XCTAssertEqual(2, response.endpoints.count)

                try! service.shutdown().wait()
            })
        }
    }

    func testJoinFirstNodeWithConflicts() throws {
        let nodeAddress = addressFromParts("127.0.0.1", 1234)
        let nodeUUID = nodeIdFromUUID(UUID())
        let joiningNodeAddress = addressFromParts("localhost", 1235)

        withTestClient { client in
            withTestServer(nodeAddress, { server in
                let service = try! createMembershipService(serverAddress: nodeAddress, client: client, server: server, nodeId: nodeUUID)

                // Try to join with the same host details as the server and different UUID
                let joinResponse1 = try! sendJoinMessage(client: client, nodeAddress: nodeAddress, joiningNodeAddress: nodeAddress, joiningNodeId: nodeIdFromUUID(UUID()))
                XCTAssertEqual(JoinStatusCode.hostnameAlreadyInRing, joinResponse1.statusCode)
                XCTAssertEqual(0, joinResponse1.endpoints.count)
                XCTAssertEqual(0, joinResponse1.identifiers.count)

                // Try to join with a different host and the same UUID as the server
                let joinResponse2 = try! sendJoinMessage(client: client, nodeAddress: nodeAddress, joiningNodeAddress: joiningNodeAddress, joiningNodeId: nodeUUID)
                XCTAssertEqual(JoinStatusCode.uuidAlreadyInRing, joinResponse2.statusCode)
                XCTAssertEqual(0, joinResponse2.endpoints.count)
                XCTAssertEqual(0, joinResponse2.identifiers.count)

                try! service.shutdown().wait()
            })
        }
    }

    func testJoinWithSingleNodeBootstrap() throws {
        let nodeId = nodeIdFromUUID(UUID())
        let nodeAddress = addressFromParts("127.0.0.1", 1234)
        let joiningNodeAddress = addressFromParts("127.0.0.1", 1235)
        let joiningNodeId = nodeIdFromUUID(UUID())
        let view = MembershipView(K: settings.K)
        try view.ringAdd(node: nodeAddress, nodeId: nodeId)


        withTestClient { client in
            withTestServer(nodeAddress, { server in
                let service = try! createMembershipService(serverAddress: nodeAddress, client: client, server: server, nodeId: nodeId)

                let response = try! sendJoinMessage(client: client, nodeAddress: nodeAddress, joiningNodeAddress: joiningNodeAddress, joiningNodeId: joiningNodeId)
                XCTAssertEqual(JoinStatusCode.safeToJoin, response.statusCode)
                XCTAssertEqual(2, response.endpoints.count)
                // don't try to check things against the initial view we have provided as the view is not thread-safe and the latest modifications are very likely
                // not visible on the current thread
                XCTAssertEqual(response.endpoints, try! service.getMemberList())

                try! service.shutdown().wait()

            })
        }
    }

    private func sendJoinMessage(client: MessagingClient, nodeAddress: Endpoint, joiningNodeAddress: Endpoint, joiningNodeId: NodeId) throws -> JoinResponse {
        let msg = RapidRequest.with {
            $0.joinMessage = JoinMessage.with {
                $0.sender = joiningNodeAddress
                $0.nodeID = joiningNodeId
            }
        }
        return try client.sendMessage(recipient: nodeAddress, msg: msg).wait().joinResponse
    }

    private func createMembershipService(serverAddress: Endpoint, client: MessagingClient, server: MessagingServer, initialView: MembershipView? = nil, nodeId: NodeId = nodeIdFromUUID(UUID())) throws -> RapidMembershipService {
        let view = initialView ?? MembershipView(K: settings.K)
        try view.ringAdd(node: serverAddress, nodeId: nodeId)
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
        ("testJoinFirstNodeWithConflicts", testJoinFirstNodeWithConflicts),
        ("testJoinWithSingleNodeBootstrap", testJoinWithSingleNodeBootstrap)
    ]

}