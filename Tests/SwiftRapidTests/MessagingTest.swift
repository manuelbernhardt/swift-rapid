import XCTest
import NIO
import NIOConcurrencyHelpers
import Dispatch
@testable import SwiftRapid

class MessagingTest: XCTestCase, TestServerMessaging, TestClientMessaging {

    var eventLoopGroup: MultiThreadedEventLoopGroup? = nil
    var clientSettings = Settings()

    var settings = Settings()
    var provider: ActorRefProvider? = nil

    override func setUp() {
        eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 4)
        provider = ActorRefProvider(group: eventLoopGroup!)
        clientSettings = Settings()
        settings = Settings()
    }

    override func tearDown() {
        try! eventLoopGroup?.syncShutdownGracefully()
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

               try! service.shutdown(el: eventLoopGroup!.next()).wait()
            }, disableTestMembershipService: true)
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

                try! service.shutdown(el: eventLoopGroup!.next()).wait()
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
                XCTAssertEqual(response.endpoints, try! service.getMemberList().wait())

                try! service.shutdown(el: eventLoopGroup!.next()).wait()
            })
        }
    }

    func testBootstrapAndProbe() throws {
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

                let probe = RapidRequest.with {
                    $0.probeMessage = ProbeMessage()
                }
                let probeResponse: RapidResponse = try! client.sendMessage(recipient: nodeAddress, msg: probe).wait()
                XCTAssertEqual(NodeStatus.ok, probeResponse.probeResponse.status)

                try! service.shutdown(el: eventLoopGroup!.next()).wait()
            })
        }
    }

    /// Tests a race between probe messages from nodes that have already adopted the new view and a joining node being probed that hasn't received the join response yet
    func testProbeAndThenBootstrap() throws {
        let node1Address = addressFromParts("127.0.0.1", 1234)
        let node2Address = addressFromParts("127.0.0.1", 1235)

        let view = MembershipView(K: settings.K)
        try view.ringAdd(node: node1Address, nodeId: nodeIdFromUUID(UUID()))
        try view.ringAdd(node: node2Address, nodeId: nodeIdFromUUID(UUID())) // this way node1 will observe node2

        withTestServer(node1Address, { server1 in
            withTestServer(node2Address, { server2 in
                withTestClient { client in
                    let service = try! createMembershipService(serverAddress: node1Address, client: client, server: server1, initialView: view)

                    let probe = RapidRequest.with {
                        $0.probeMessage = ProbeMessage()
                    }
                    let probe1Response = try! client.sendMessage(recipient: node1Address, msg: probe).wait().probeResponse
                    XCTAssertEqual(NodeStatus.ok, probe1Response.status)

                    let probe2Response = try! client.sendMessage(recipient: node2Address, msg: probe).wait().probeResponse
                    XCTAssertEqual(NodeStatus.bootstrapping, probe2Response.status)

                    try! service.shutdown(el: eventLoopGroup!.next()).wait()
                }
            })
        })

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
        if (initialView == nil) {
            try view.ringAdd(node: serverAddress, nodeId: nodeId)
        }
        let broadcaster = UnicastToAllBroadcaster(client: client, el: eventLoopGroup!.next())
        let failureDetectorProvider = AdaptiveAccrualFailureDetectorProvider(selfEndpoint: serverAddress, messagingClient: client, provider: provider!, settings: settings, el: eventLoopGroup!.next())
        let membershipService = try RapidMembershipService(selfEndpoint: serverAddress, settings: settings, view: view, failureDetectorProvider: failureDetectorProvider,
                broadcaster: broadcaster, messagingClient: client, allMetadata: [serverAddress: Metadata()],
                subscriptions: [],
                provider: provider!, el: eventLoopGroup!.next())
        server.onMembershipServiceInitialized(membershipService: membershipService)
        try server.start()
        return membershipService
    }

    static var allTests = [
        ("testJoinFirstNode", testJoinFirstNode),
        ("testJoinFirstNodeWithConflicts", testJoinFirstNodeWithConflicts),
        ("testJoinWithSingleNodeBootstrap", testJoinWithSingleNodeBootstrap),
        ("testBootstrapAndProbe", testBootstrapAndProbe),
        ("testProbeAndThenBootstrap", testProbeAndThenBootstrap)
    ]

}