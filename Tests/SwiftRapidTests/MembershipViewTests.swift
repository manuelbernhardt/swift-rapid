import XCTest
@testable import SwiftRapid

final class MembershipViewTests: XCTestCase {

    let K: Int = 10

    func testOneRingAddition() throws {
        let view = MembershipView(K: K)
        let address = addressFromParts("127.0.0.1", 1234)
        let uuid = UUID()
        XCTAssertNoThrow(try? view.ringAdd(node: address, nodeId: nodeIdFromUUID(uuid)))

        for k in 0..<K {
            let ring = view.getRing(k: k)
            XCTAssertEqual(1, ring.count)
            ring.forEach { node in
                XCTAssertEqual(address, node)
            }
        }
    }


    func testMultipleRingAdditions() throws {
        let view = MembershipView(K: K)
        let numNodes = 10
        for i in 0..<numNodes {
            XCTAssertNoThrow(try? view.ringAdd(node: addressFromParts("127.0.0.1", i), nodeId: nodeIdFromUUID(UUID())))
        }
        XCTAssertEqual(numNodes, view.getMembershipSize())

        for k in 0..<K {
            let ring = view.getRing(k: k)
            XCTAssertEqual(numNodes, ring.count)
        }
    }

    func testRingReAdditions() throws {
        let view = MembershipView(K: K)
        let numNodes = 10
        for i in 0..<numNodes {
            XCTAssertNoThrow(try? view.ringAdd(node: addressFromParts("127.0.0.1", i), nodeId: nodeIdFromUUID(UUID())))
        }
        for k in 0..<K {
            let ring = view.getRing(k: k)
            XCTAssertEqual(numNodes, ring.count)
        }

        var errorCount = 0
        for i in 0..<numNodes {
            do {
                try view.ringAdd(node: addressFromParts("127.0.0.1", i), nodeId: nodeIdFromUUID(UUID()))
            } catch MembershipViewError.NodeAlreadyInRingError(_) {
                errorCount += 1
            }
        }
        XCTAssertEqual(numNodes, errorCount)
    }

    func testRejectInvalidRingDeletions() throws {
        let view = MembershipView(K: K)
        let numNodes = 10
        var errorCount = 0
        for i in 0..<numNodes {
            do {
                try view.ringDelete(node: addressFromParts("127.0.0.1", i))
            } catch MembershipViewError.NodeNotInRingError(_) {
                errorCount += 1
            }
        }
        XCTAssertEqual(numNodes, errorCount)
    }

    func testRingAdditionsAndDeletions() throws {
        let view = MembershipView(K: K)
        let numNodes = 10
        for i in 0..<numNodes {
            try! view.ringAdd(node: addressFromParts("127.0.0.1", i), nodeId: nodeIdFromUUID(UUID()))
        }
        for i in 0..<numNodes {
            try! view.ringDelete(node: addressFromParts("127.0.0.1", i))
        }

        for k in 0..<K {
            let ring = view.getRing(k: k)
            XCTAssertEqual(0, ring.count)
        }
    }

    func testMonitoringRelationshipEmptyCluster() throws {
        let view = MembershipView(K: K)

        let node = addressFromParts("127.0.0.1", 1)

        XCTAssertThrowsError(try view.getSubjectsOf(node: node)) { error in
            XCTAssertEqual(error as! MembershipViewError, MembershipViewError.NodeNotInRingError(node))
        }
        XCTAssertThrowsError(try view.getObserversOf(node)) { error in
            XCTAssertEqual(error as! MembershipViewError, MembershipViewError.NodeNotInRingError(node))
        }
    }

    func testMonitoringRelationshipSingleNodeCluster() throws {
        let view = MembershipView(K: K)

        let node = addressFromParts("127.0.0.1", 1)
        try view.ringAdd(node: node, nodeId: nodeIdFromUUID(UUID()))
        XCTAssertEqual(0, try! view.getSubjectsOf(node: node).count)
        XCTAssertEqual(0, try! view.getObserversOf(node).count)
    }

    func testMonitoringRelationshipTwoNodeCluster() throws {
        let view = MembershipView(K: K)

        let node1 = addressFromParts("127.0.0.1", 1)
        let node2 = addressFromParts("127.0.0.1", 2)
        try view.ringAdd(node: node1, nodeId: nodeIdFromUUID(UUID()))
        try view.ringAdd(node: node2, nodeId: nodeIdFromUUID(UUID()))

        XCTAssertEqual(K, try! view.getSubjectsOf(node: node1).count)
        XCTAssertEqual(K, try! view.getObserversOf(node1).count)
        XCTAssertEqual(1, try! Set(view.getSubjectsOf(node: node1)).count)
        XCTAssertEqual(1, try! Set(view.getObserversOf(node1)).count)
    }

    func testMonitoringRelationshipThreeNodesWithDelete() throws {
        let view = MembershipView(K: K)
        let node1 = addressFromParts("127.0.0.1", 1)
        let node2 = addressFromParts("127.0.0.1", 2)
        let node3 = addressFromParts("127.0.0.1", 3)
        try view.ringAdd(node: node1, nodeId: nodeIdFromUUID(UUID()))
        try view.ringAdd(node: node2, nodeId: nodeIdFromUUID(UUID()))
        try view.ringAdd(node: node3, nodeId: nodeIdFromUUID(UUID()))

        XCTAssertEqual(3, view.getMembershipSize())

        func checkRelationships(node: Endpoint, expectedCount: Int) {
            XCTAssertEqual(K, try! view.getSubjectsOf(node: node).count)
            XCTAssertEqual(K, try! view.getObserversOf(node).count)
            XCTAssertEqual(expectedCount, try! Set(view.getSubjectsOf(node: node)).count)
            XCTAssertEqual(expectedCount, try! Set(view.getObserversOf(node)).count)
        }

        checkRelationships(node: node1, expectedCount: 2)
        checkRelationships(node: node2, expectedCount: 2)
        checkRelationships(node: node3, expectedCount: 2)

        try view.ringDelete(node: node2)

        checkRelationships(node: node1, expectedCount: 1)
        checkRelationships(node: node3, expectedCount: 1)
    }

    func testMonitoringRelationshipMultipleNodes() throws {
        let view = MembershipView(K: K)
        // TODO test with 1000 nodes once the sorting perf issue has been sorted out
        let numNodes = 100
        var nodes = [Endpoint]()
        for i in 0..<numNodes {
            let node = addressFromParts("127.0.0.1", i)
            nodes.append(node)
            XCTAssertNoThrow(try? view.ringAdd(node: node, nodeId: nodeIdFromUUID(UUID())))
        }

        for i in 0..<numNodes {
            let numSubjects = try view.getSubjectsOf(node: nodes[i]).count
            let numObservers = try view.getObserversOf(nodes[i]).count
            XCTAssertEqual(K, numSubjects)
            XCTAssertEqual(K, numObservers)
        }
    }

    func testMonitoringRelationshipDuringBootstrap() throws {
        let view = MembershipView(K : K)
        let node = addressFromParts("127.0.0.1", 1234)
        try view.ringAdd(node: node, nodeId: nodeIdFromUUID(UUID()))

        let joiningNode = addressFromParts("127.0.0.1", 1235)
        let expectedObservers = view.getExpectedObserversOf(node: joiningNode)
        XCTAssertEqual(K, expectedObservers.count)
        XCTAssertEqual(1, Set(expectedObservers).count)
        XCTAssertEqual(node, expectedObservers[0])
    }

    func testMonitoringRelationshipDuringBootstrapMultiple() throws {
        let view = MembershipView(K : K)
        let numNodes = 20
        let basePort = 1234
        let joiningNode = addressFromParts("127.0.0.1", basePort)
        var numObservers = 0
        for i in 0..<numNodes {
            let node = addressFromParts("127.0.0.1", basePort + i)
            try view.ringAdd(node: node, nodeId: nodeIdFromUUID(UUID()))
        }
        let numObserversActual = view.getExpectedObserversOf(node: joiningNode).count

        // we could compare against i + 1 but that condition is not guaranteed
        // to hold true since we are not constructing deterministic expanders
        XCTAssertTrue(numObservers <= numObserversActual)
        numObservers = numObserversActual

        XCTAssertEqual(K, numObservers)
    }

    func testUniqueIdConstraints() throws {
        let view = MembershipView(K : K)
        let node1 = addressFromParts("127.0.0.1", 1234)
        let uid1 = UUID()
        let nodeId1 = nodeIdFromUUID(uid1)
        try view.ringAdd(node: node1, nodeId: nodeId1)

        let node2 = addressFromParts("127.0.0.1", 1234)
        let nodeId2: NodeId = nodeIdFromUUID(uid1)

        // same host, same UID
        XCTAssertThrowsError(try view.ringAdd(node: node2, nodeId: nodeId2)) { error in
            XCTAssertEqual(error as! MembershipViewError, MembershipViewError.UUIDAlreadySeenError(node2, nodeId2))
        }

        // same host, different UID
        XCTAssertThrowsError(try view.ringAdd(node: node2, nodeId: nodeIdFromUUID(UUID()))) { error in
            XCTAssertEqual(error as! MembershipViewError, MembershipViewError.NodeAlreadyInRingError(node2))
        }

        // different host, same UID
        let node3 = addressFromParts("127.0.0.1", 1235)
        XCTAssertThrowsError(try view.ringAdd(node: node3, nodeId: nodeId2)) { error in
            XCTAssertEqual(error as! MembershipViewError, MembershipViewError.UUIDAlreadySeenError(node3, nodeId2))
        }

        try view.ringAdd(node: node3, nodeId: nodeIdFromUUID(UUID()))

        XCTAssertEqual(2, view.getMembershipSize())
    }

    func testUniqueIdConstraintsWithDeletions() throws {
        let view = MembershipView(K : K)

        let node1 = addressFromParts("127.0.0.1", 1234)
        let nodeId1 = nodeIdFromUUID(UUID())
        try view.ringAdd(node: node1, nodeId: nodeId1)

        let node2 = addressFromParts("127.0.0.1", 1235)
        let nodeId2: NodeId = nodeIdFromUUID(UUID())
        try view.ringAdd(node: node2, nodeId: nodeId2)

        try view.ringDelete(node: node2)
        XCTAssertEqual(1, view.getMembershipSize())

        XCTAssertThrowsError(try view.ringAdd(node: node2, nodeId: nodeId2)) { error in
            XCTAssertEqual(error as! MembershipViewError, MembershipViewError.UUIDAlreadySeenError(node2, nodeId2))
        }

        XCTAssertEqual(1, view.getMembershipSize())

        try view.ringAdd(node: node2, nodeId: nodeIdFromUUID(UUID()))
        XCTAssertEqual(2, view.getMembershipSize())
    }

    func testNodeConfigurationChange() throws {
        let view = MembershipView(K : K)
        let numNodes = 100 // TODO 1000 when perf allows
        var configurationIds = Set<UInt64>()
        for i in 0..<numNodes {
            let node = addressFromParts("127.0.0.1", i)
            try view.ringAdd(node: node, nodeId: nodeIdFromUUID(UUID()))
            configurationIds.insert(view.getCurrentConfigurationId())
        }
        XCTAssertEqual(numNodes, configurationIds.count)
    }

    func testNodeConfigurationChangeAcrossViews() throws {
        let view1 = MembershipView(K : K)
        let view2 = MembershipView(K : K)
        let numNodes = 100 // TODO 1000 when perf allows

        var nodeIds = [Endpoint : UUID]()

        var configurationIds1 = [UInt64]()
        var configurationIds2 = [UInt64]()

        for i in 0..<numNodes {
            let node = addressFromParts("127.0.0.1", i)
            let uuid = UUID()
            nodeIds[node] = uuid

            try view1.ringAdd(node: node, nodeId: nodeIdFromUUID(uuid))
            configurationIds1.append(view1.getCurrentConfigurationId())
        }

        for i in (0..<numNodes).reversed() {
            let node = addressFromParts("127.0.0.1", i)
            let uuid = nodeIds[node]!

            try view2.ringAdd(node: node, nodeId: nodeIdFromUUID(uuid))
            configurationIds2.append(view2.getCurrentConfigurationId())
        }

        XCTAssertEqual(numNodes, configurationIds1.count)
        XCTAssertEqual(numNodes, configurationIds2.count)

        // given the nodes are added in opposite order, only the last configuration IDs should be identical
        // across both views
        for i in 0 ..< numNodes - 1 {
            XCTAssertNotEqual(configurationIds1[i], configurationIds2[i])
        }
        XCTAssertEqual(configurationIds1.last, configurationIds2.last)
    }

    static var allTests = [
        ("testOneRingAddition", testOneRingAddition),
        ("testMultipleRingAdditions", testMultipleRingAdditions),
        ("testRingReAdditions", testRingReAdditions),
        ("testRejectInvalidRingDeletions", testRejectInvalidRingDeletions),
        ("testRingAdditionsAndDeletions", testRingAdditionsAndDeletions),
        ("testMonitoringRelationshipSingleNodeCluster", testMonitoringRelationshipSingleNodeCluster),
        ("testMonitoringRelationshipEmptyCluster", testMonitoringRelationshipEmptyCluster),
        ("testMonitoringRelationshipTwoNodeCluster", testMonitoringRelationshipTwoNodeCluster),
        ("testMonitoringRelationshipThreeNodesWithDelete", testMonitoringRelationshipThreeNodesWithDelete),
        ("testMonitoringRelationshipMultipleNodes", testMonitoringRelationshipMultipleNodes),
        ("testMonitoringRelationshipDuringBootstrap", testMonitoringRelationshipDuringBootstrap),
        ("testUniqueIdConstraints", testUniqueIdConstraints),
        ("testUniqueIdConstraintsWithDeletions", testUniqueIdConstraintsWithDeletions),
        ("testNodeConfigurationChange", testNodeConfigurationChange),
        ("testNodeConfigurationChangeAcrossViews", testNodeConfigurationChangeAcrossViews),
    ]
}
