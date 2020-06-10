import XCTest
import NIO
@testable import SwiftRapid

class FastPaxosWithoutFallbackTests: XCTestCase {
    let K = 10
    let H = 8
    let L = 3

    var group: EventLoopGroup? = nil

    override func setUp() {
        super.setUp()
        group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
    }

    override func tearDown() {
        super.tearDown()
        try! group?.syncShutdownGracefully()

    }

    func testQuorumNoConflicts() throws {
        let params = [
            (6, 5), (48, 37), (50, 38), (100, 76), (102, 77), // even N
            (5, 4), (51, 39), (49, 37), (99, 75), (101, 76) // odd N
        ]

        try params.forEach { try fastQuorumTestNoConflicts(N: $0.0, quorum: $0.1) }
    }

    func testQuorumWithConflicts() throws {
        // One conflicting message. Must lead to decision.
        let oneConflictingMessage = [
            (6, 5, 1, true), (48, 37, 1, true), (50, 38, 1, true), (100, 76, 1, true), (102, 77, 1, true)
        ]

        // Boundary case: F conflicts, and N-F non-conflicts. Must lead to decisions.
        let boundaryCase = [
            (48, 37, 11, true), (50, 38, 12, true), (100, 76, 24, true), (102, 77, 25, true)
        ]

        // More conflicts than Fast Paxos quorum size. These must not lead to decisions.
        let tooManyConflicts = [
            (6, 5, 2, false), (48, 37, 14, false), (50, 38, 13, false), (100, 76, 25, false), (102, 77, 26, false)
        ]

        try (oneConflictingMessage + boundaryCase + tooManyConflicts).forEach {
            try fastQuorumTestWithConflicts(N: $0.0, quorum: $0.1, numConflicts: $0.2, changeExpected: $0.3)
        }
    }

    /// In this test we simulate the proposal to remove one node from the view
    /// We test for different membership sizes and quorums (i.e. amount of nodes that submitted a vote)
    func fastQuorumTestNoConflicts(N: Int, quorum: Int) throws {
        let basePort = 1234
        let node = addressFromParts("127.0.0.1", basePort)
        let proposalNode = addressFromParts("127.0.0.1", basePort + 1)
        let view = try createView(basePort: basePort, N: N)
        let settings = Settings()
        let broadcaster = TestBroadcaster(eventLoop: group!.next())

        var proposal = [Endpoint]()
        var callbackInvoked = false
        let callback = { (endpoints: [Endpoint]) in
            callbackInvoked = true
            proposal = endpoints
        }
        let fastPaxos = FastPaxos(selfEndpoint: node,
                configurationId: view.getCurrentConfigurationId(),
                membershipSize: view.getMembershipSize(),
                decisionCallback:callback,
                broadcaster: broadcaster,
                settings: settings
        )


        for i in 0..<quorum-1 {
            // this will cause the proposalNode to be removed from the membership once the quorum is reached
            let msg = FastRoundPhase2bMessage.with {
                $0.endpoints = [proposalNode]
                $0.sender = addressFromParts("127.0.0.1", i)
                $0.configurationID = view.getCurrentConfigurationId()
            }
            fastPaxos.handleFastRoundProposal(proposalMessage: msg)
            XCTAssertFalse(callbackInvoked)
        }

        let lastMsg = FastRoundPhase2bMessage.with {
            $0.endpoints = [proposalNode]
            $0.sender = addressFromParts("127.0.0.1", quorum - 1)
            $0.configurationID = view.getCurrentConfigurationId()
        }

        fastPaxos.handleFastRoundProposal(proposalMessage: lastMsg)
        XCTAssertEqual(1, proposal.count)
        XCTAssertEqual(proposalNode, proposal.first)
    }

    /// In this test we simulate two conflicting proposals, each for one node to be removed from the membership view
    /// We test for different membership sizes, quorums and amounts of conflicting messages.
    /// Depending on the constellation we may or may not reach consensus
    func fastQuorumTestWithConflicts(N: Int, quorum: Int, numConflicts: Int, changeExpected: Bool) throws {
        let basePort = 1234
        let node = addressFromParts("127.0.0.1", basePort)
        let proposalNode = addressFromParts("127.0.0.1", basePort + 1)
        let proposalNodeConflict = addressFromParts("127.0.0.1", basePort + 2)
        let view = try createView(basePort: basePort, N: N)
        let settings = Settings()
        let broadcaster = TestBroadcaster(eventLoop: group!.next())

        var proposal = [Endpoint]()
        var callbackInvoked = false
        let callback = { (endpoints: [Endpoint]) in
            callbackInvoked = true
            proposal = endpoints
        }

        let fastPaxos = FastPaxos(selfEndpoint: node,
                configurationId: view.getCurrentConfigurationId(),
                membershipSize: view.getMembershipSize(),
                decisionCallback:callback,
                broadcaster: broadcaster,
                settings: settings
        )

        for i in 0..<numConflicts {
            let conflictMsg = FastRoundPhase2bMessage.with {
                $0.endpoints = [proposalNodeConflict]
                $0.sender = addressFromParts("127.0.0.1", i)
                $0.configurationID = view.getCurrentConfigurationId()
            }
            fastPaxos.handleFastRoundProposal(proposalMessage: conflictMsg)
            // no proposal yet
            XCTAssertFalse(callbackInvoked)
        }
        let nonConflictCount = min(numConflicts + quorum - 1, N - 1)
        for i in numConflicts..<nonConflictCount {
            let msg = FastRoundPhase2bMessage.with {
                $0.endpoints = [proposalNode]
                $0.sender = addressFromParts("127.0.0.1", i)
                $0.configurationID = view.getCurrentConfigurationId()
            }
            fastPaxos.handleFastRoundProposal(proposalMessage: msg)
            // no proposal yet
            XCTAssertFalse(callbackInvoked)
        }

        // let it cross the quorum for the normal proposal
        let lastMsg = FastRoundPhase2bMessage.with {
            $0.endpoints = [proposalNode]
            $0.sender = addressFromParts("127.0.0.1", nonConflictCount)
            $0.configurationID = view.getCurrentConfigurationId()
        }
        fastPaxos.handleFastRoundProposal(proposalMessage: lastMsg)
        XCTAssertTrue(callbackInvoked || !changeExpected)
        if (changeExpected) {
            XCTAssertEqual(1, proposal.count)
        } else {
            XCTAssertEqual(0, proposal.count)
        }

    }

    private func createView(basePort: Int, N: Int) throws -> MembershipView {
        let view = MembershipView(K: K)
        for port in basePort..<basePort+N {
            try view.ringAdd(node: addressFromParts("127.0.0.1", port), nodeId: nodeIdFromUUID(UUID()))
        }
        return view
    }
}

class TestBroadcaster: Broadcaster {

    private let ev: EventLoop

    init(eventLoop: EventLoop) {
        self.ev = eventLoop
    }

    var didBroadcast: Bool = false

    func broadcast(request: RapidRequest) -> [EventLoopFuture<RapidResponse>] {
        didBroadcast = true
        return [ev.submit { () -> RapidResponse in
            RapidResponse()
        }]
    }

    func setMembership(recipients: [Endpoint]) {
    }
}