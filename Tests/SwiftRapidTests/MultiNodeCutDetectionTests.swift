import XCTest
@testable import SwiftRapid

final class MultiNodeCutDetectionTests: XCTestCase {

    let K = 10
    let H = 8
    let L = 2
    let ConfigurationId: UInt64 = 0

    func testCutDetection() throws {
        let cd = try MultiNodeCutDetector(K: K, H: H, L: L)
        let destination = addressFromParts("127.0.0.1", 1234)

        for i in 0..<H-1 {
            let alert = buildAlert(addressFromParts("127.0.0.1", i + 1), destination, i)
            let proposal = cd.aggregate(alert: alert)
            XCTAssertEqual(0, proposal.count)
            XCTAssertEqual(0, cd.getProposalCount())
        }

        // one more alert to reach the H threshold
        let alert = buildAlert(addressFromParts("127.0.0.1", H), destination, H - 1)

        let proposal = cd.aggregate(alert: alert)
        XCTAssertEqual(1, proposal.count)
        XCTAssertEqual(1, cd.getProposalCount())
    }

    func testCutDetectionWithOneBlocker() throws {
        let cd = try MultiNodeCutDetector(K: K, H: H, L: L)
        let destination1 = addressFromParts("127.0.0.1", 1234)
        let destination2 = addressFromParts("127.0.0.1", 1235)

        // generate H-1 alerts for destination1
        for i in 0..<H-1 {
            let alert = buildAlert(addressFromParts("127.0.0.1", i + 1), destination1, i)
            let proposal = cd.aggregate(alert: alert)
            XCTAssertEqual(0, proposal.count)
            XCTAssertEqual(0, cd.getProposalCount())
        }

        // generate H-1 alerts for destination2
        for i in 0..<H-1 {
            let alert = buildAlert(addressFromParts("127.0.0.1", i + 1), destination2, i)
            let proposal = cd.aggregate(alert: alert)
            XCTAssertEqual(0, proposal.count)
            XCTAssertEqual(0, cd.getProposalCount())
        }

        // now let alerts for destination1 reach H
        let alert1 = buildAlert(addressFromParts("127.0.0.1", H), destination1, H - 1)
        let proposal = cd.aggregate(alert: alert1)

        // since there are alerts below H there should be no proposal yet
        XCTAssertEqual(0, proposal.count)
        XCTAssertEqual(0, cd.getProposalCount())

        // let it reach H for destination2
        let alert2 = buildAlert(addressFromParts("127.0.0.1", H), destination2, H - 1)
        let proposal2 = cd.aggregate(alert: alert2)

        // there should be two endpoints (destination1 and destination2) in the proposal
        XCTAssertEqual(2, proposal2.count)
        XCTAssertEqual(1, cd.getProposalCount())
    }

    func testCutDetectionWithThreeBlockers() throws {
        let cd = try MultiNodeCutDetector(K: K, H: H, L: L)
        let destination1 = addressFromParts("127.0.0.1", 1234)
        let destination2 = addressFromParts("127.0.0.1", 1235)
        let destination3 = addressFromParts("127.0.0.1", 1236)

        // generate H-1 alerts for destination1
        for i in 0..<H-1 {
            let alert = buildAlert(addressFromParts("127.0.0.1", i + 1), destination1, i)
            let proposal = cd.aggregate(alert: alert)
            XCTAssertEqual(0, proposal.count)
            XCTAssertEqual(0, cd.getProposalCount())
        }

        // generate H-1 alerts for destination2
        for i in 0..<H-1 {
            let alert = buildAlert(addressFromParts("127.0.0.1", i + 1), destination2, i)
            let proposal = cd.aggregate(alert: alert)
            XCTAssertEqual(0, proposal.count)
            XCTAssertEqual(0, cd.getProposalCount())
        }

        // generate H-1 alerts for destination3
        for i in 0..<H-1 {
            let alert = buildAlert(addressFromParts("127.0.0.1", i + 1), destination3, i)
            let proposal = cd.aggregate(alert: alert)
            XCTAssertEqual(0, proposal.count)
            XCTAssertEqual(0, cd.getProposalCount())
        }

        // now let alerts for destination1 reach H
        let alert1 = buildAlert(addressFromParts("127.0.0.1", H), destination1, H - 1)
        let proposal1 = cd.aggregate(alert: alert1)
        XCTAssertEqual(0, proposal1.count)
        XCTAssertEqual(0, cd.getProposalCount())

        // now let alerts for destination2 reach H
        let alert2 = buildAlert(addressFromParts("127.0.0.1", H), destination2, H - 1)
        let proposal2 = cd.aggregate(alert: alert2)
        XCTAssertEqual(0, proposal2.count)
        XCTAssertEqual(0, cd.getProposalCount())

        // now let alerts for destination3 reach H
        let alert3 = buildAlert(addressFromParts("127.0.0.1", H), destination3, H - 1)
        let proposal3 = cd.aggregate(alert: alert3)
        XCTAssertEqual(3, proposal3.count)
        XCTAssertEqual(1, cd.getProposalCount())
    }

    func testCutDetectionWithThreeBlockersCrossingH() throws {
        let cd = try MultiNodeCutDetector(K: K, H: H, L: L)
        let destination1 = addressFromParts("127.0.0.1", 1234)
        let destination2 = addressFromParts("127.0.0.1", 1235)
        let destination3 = addressFromParts("127.0.0.1", 1236)

        // generate H-1 alerts for destination1
        for i in 0..<H-1 {
            let alert = buildAlert(addressFromParts("127.0.0.1", i + 1), destination1, i)
            let proposal = cd.aggregate(alert: alert)
            XCTAssertEqual(0, proposal.count)
            XCTAssertEqual(0, cd.getProposalCount())
        }

        // generate H-1 alerts for destination2
        for i in 0..<H-1 {
            let alert = buildAlert(addressFromParts("127.0.0.1", i + 1), destination2, i)
            let proposal = cd.aggregate(alert: alert)
            XCTAssertEqual(0, proposal.count)
            XCTAssertEqual(0, cd.getProposalCount())
        }

        // generate H-1 alerts for destination3
        for i in 0..<H-1 {
            let alert = buildAlert(addressFromParts("127.0.0.1", i + 1), destination3, i)
            let proposal = cd.aggregate(alert: alert)
            XCTAssertEqual(0, proposal.count)
            XCTAssertEqual(0, cd.getProposalCount())
        }

        // now let alerts for destination1 cross H
        let alert1 = buildAlert(addressFromParts("127.0.0.1", H), destination1, H - 1)
        let alert2 = buildAlert(addressFromParts("127.0.0.1", H + 1), destination1, H)

        let _ = cd.aggregate(alert: alert1)
        let proposal1 = cd.aggregate(alert: alert2)
        XCTAssertEqual(0, proposal1.count)
        XCTAssertEqual(0, cd.getProposalCount())

        // now let alerts for destination2 cross H
        let alert3 = buildAlert(addressFromParts("127.0.0.1", H), destination2, H - 1)
        let alert4 = buildAlert(addressFromParts("127.0.0.1", H + 1), destination2, H)
        let _ = cd.aggregate(alert: alert3)
        let proposal2 = cd.aggregate(alert: alert4)
        XCTAssertEqual(0, proposal2.count)
        XCTAssertEqual(0, cd.getProposalCount())

        // now let alerts for destination3 reach H
        let alert5 = buildAlert(addressFromParts("127.0.0.1", H), destination3, H - 1)
        let proposal3 = cd.aggregate(alert: alert5)
        XCTAssertEqual(3, proposal3.count)
        XCTAssertEqual(1, cd.getProposalCount())
    }

    func testCutDetectionBelowL() throws {
        let cd = try MultiNodeCutDetector(K: K, H: H, L: L)
        let destination1 = addressFromParts("127.0.0.1", 1234)
        let destination2 = addressFromParts("127.0.0.1", 1235)
        let destination3 = addressFromParts("127.0.0.1", 1236)

        // generate H-1 alerts for destination1
        for i in 0..<H-1 {
            let alert = buildAlert(addressFromParts("127.0.0.1", i + 1), destination1, i)
            let proposal = cd.aggregate(alert: alert)
            XCTAssertEqual(0, proposal.count)
            XCTAssertEqual(0, cd.getProposalCount())
        }

        // generate L-1 alerts for destination2
        for i in 0..<L-1 {
            let alert = buildAlert(addressFromParts("127.0.0.1", i + 1), destination2, i)
            let proposal = cd.aggregate(alert: alert)
            XCTAssertEqual(0, proposal.count)
            XCTAssertEqual(0, cd.getProposalCount())
        }

        // generate H-1 alerts for destination3
        for i in 0..<H-1 {
            let alert = buildAlert(addressFromParts("127.0.0.1", i + 1), destination3, i)
            let proposal = cd.aggregate(alert: alert)
            XCTAssertEqual(0, proposal.count)
            XCTAssertEqual(0, cd.getProposalCount())
        }

        // now let alerts for destination1 reach H
        let alert1 = buildAlert(addressFromParts("127.0.0.1", H), destination1, H - 1)
        let proposal1 = cd.aggregate(alert: alert1)
        XCTAssertEqual(0, proposal1.count)
        XCTAssertEqual(0, cd.getProposalCount())

        // now let alerts for destination3 reach H
        // we should emit a proposal as alerts for destination 2 are filtered out / seen as noise
        let alert2 = buildAlert(addressFromParts("127.0.0.1", H), destination3, H - 1)
        let proposal2 = cd.aggregate(alert: alert2)
        XCTAssertEqual(2, proposal2.count)
        XCTAssertEqual(1, cd.getProposalCount())
    }

    func testCutDetectionBatch() throws {
        let cd = try MultiNodeCutDetector(K: K, H: H, L: L)
        let destination1 = addressFromParts("127.0.0.1", 1234)
        let destination2 = addressFromParts("127.0.0.1", 1235)
        let destination3 = addressFromParts("127.0.0.1", 1236)

        let endpoints = [destination1, destination2, destination3]

        let proposal = endpoints.flatMap { endpoint in
            (0..<K).flatMap { ringNumber in
                cd.aggregate(alert: self.buildAlert(addressFromParts("127.0.0.1", 1), endpoint, ringNumber))
            }
        }

        XCTAssertEqual(3, proposal.count)
    }

    func testCutDetectionLinkInvalidation() throws {
        let view = MembershipView(K: K)
        let cd = try MultiNodeCutDetector(K: K, H: H, L: L)
        let numNodes = 30
        let endpoints = (0..<numNodes).map { i in
            addressFromParts("127.0.0.1", i)
        }
        try endpoints.forEach { endpoint in
            try view.ringAdd(node: endpoint, nodeId: nodeIdFromUUID(UUID()))
        }
        let destination = endpoints.first!
        let observers = try view.getObserversOf(destination)
        XCTAssertEqual(K, observers.count)

        // generate H-1 alerts from the observers of node destination
        for i in 0..<H-1 {
            let alert = buildAlert(addressFromParts("127.0.0.1", i + 1), destination, i)
            let proposal = cd.aggregate(alert: alert)
            XCTAssertEqual(0, proposal.count)
            XCTAssertEqual(0, cd.getProposalCount())
        }

        var failedObservers = [Endpoint]()
        // generate alerts about observers [H, K) of node destination
        for i in H-1..<K {
            let failedObserver = observers[i]
            let observersOfObserver = try view.getObserversOf(failedObserver)
            failedObservers.append(failedObserver)
            for j in 0..<K {
                let proposal = cd.aggregate(alert: buildAlert(observersOfObserver[j], failedObserver, j, EdgeStatus.down))
                XCTAssertEqual(0, proposal.count)
                XCTAssertEqual(0, cd.getProposalCount())
            }
        }

        // At this point, (K - H - 1) observers of dst will be past H, and dst will be in H - 1. Link invalidation
        // should bring the failed observers and dst to the stable region.
        let proposal = cd.invalidateFailingEdges(view: view)
        XCTAssertEqual(4, proposal.count)
        XCTAssertEqual(1, cd.getProposalCount())
        for node in proposal {
            XCTAssertTrue(failedObservers.contains(node) || node.isEqualTo(message: destination))
        }
    }




    private func buildAlert(_ src: Endpoint, _ dst: Endpoint, _ ringNumber: Int, _ status: EdgeStatus = EdgeStatus.up) -> AlertMessage {
        return AlertMessage.with {
            $0.edgeSrc = src
            $0.edgeDst = dst
            $0.edgeStatus = status
            $0.configurationID = ConfigurationId
            $0.ringNumber = [Int32(ringNumber)]
        }

    }

    static var allTests = [
        ("testCutDetection", testCutDetection),
        ("testCutDetectionWithOneBlocker", testCutDetectionWithOneBlocker),
        ("testCutDetectionWithThreeBlockers", testCutDetectionWithThreeBlockers),
        ("testCutDetectionWithThreeBlockersCrossingH", testCutDetectionWithThreeBlockersCrossingH),
        ("testCutDetectionBelowL", testCutDetectionBelowL),
    ]


}
