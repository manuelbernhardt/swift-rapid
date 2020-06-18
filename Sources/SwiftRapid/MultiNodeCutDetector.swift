import Foundation

/// The MultiNodeCutDetector acts as a filter that outputs a view change proposal about a node if:
/// - there are H reports about a node
/// - there is no other node about which there ar more than L but less than H reports
///
/// The output of this filter provides almost-everywhere agreement
///
/// This class is not thread safe
class MultiNodeCutDetector {

    // the minimum amount of observers per subject / subjects per observer
    private let K_MIN = 3

    // the number of observers per subject / subjects per observer
    private let K: Int

    // high watermark
    private let H: Int

    // low watermark
    private let L: Int

    private var proposalCount = 0
    private var updatesInProgress = 0
    private var reportsPerHost = Dictionary<Endpoint, Dictionary<Int32, Endpoint>>()
    private var proposal = Set<Endpoint>()
    private var preProposal = Set<Endpoint>()
    private var seenLinkDownEvents = false

    init(K: Int, H: Int, L: Int) throws {
        self.K = K
        self.H = H
        self.L = L
        if (H > K || L > H || K < K_MIN || L <= 0 || H <= 0) {
            throw ValidityError.InvalidArgumentError("""
                                                     Arguments do not satisfy K > H >= L >= 0:
                                                     K: \(K), H: \(H), L: \(L)
                                                     """)
        }
    }

    /// Apply a AlertMessage against the cut detector. When an update moves a host
    /// past the H threshold of reports, and no other host has between H and L reports, the
    /// method returns a view change proposal.
    public func aggregate(msg: AlertMessage) -> [Endpoint] {
        return msg.ringNumber.flatMap { aggregate(src: msg.edgeSrc, dst: msg.edgeDst, edgeStatus: msg.edgeStatus, ringNumber: $0) }
    }

    private func aggregate(src: Endpoint, dst: Endpoint, edgeStatus: EdgeStatus, ringNumber: Int32) -> [Endpoint] {
        assert(ringNumber <= K)

        seenLinkDownEvents = edgeStatus == EdgeStatus.down

        var reportsForHost: Dictionary<Int32, Endpoint> = reportsPerHost[dst, default: [Int32:Endpoint]()]

        if (reportsForHost[ringNumber] != nil) {
            // ignore the duplicate announcement
            return []
        }

        reportsForHost[ringNumber] = src

        reportsPerHost[dst] = reportsForHost

        if (reportsForHost.count == L) {
            updatesInProgress += 1
            preProposal.insert(dst)
        }

        if (reportsForHost.count == H) {
            // we received enough reports about the node to make a proposal
            // provided that there are no other nodes in unstable reporting mode (L < num reports < H)
            preProposal.remove(dst)
            proposal.insert(dst)
            updatesInProgress -= 1

            if (updatesInProgress == 0) {
                // all nodes have crossed the high watermark
                // issue a proposal
                proposalCount += 1
                let result = Array(proposal)
                proposal = Set<Endpoint>()
                return result
            }
        }

        return []
    }

    /// Invalidates edges between nodes that are failing or have failed. This step may be skipped safely
    /// when there are no failing nodes.
    public func invalidateFailingEdges(view: MembershipView) -> [Endpoint] {
        if (!seenLinkDownEvents) {
            return []
        }

        var proposalsToReturn = [Endpoint]()
        let preProposalCopy = Array(preProposal)

        preProposalCopy.forEach { node in
            let observers = try! view.isHostPresent(node) ? view.getObserversOf(node) : view.getExpectedObserversOf(node: node)
            var ringNumber: Int32 = 0
            observers.forEach { observer in
                if (proposal.contains(observer) || preProposal.contains(observer)) {
                    // implicit detection of edges between observer and node
                    let edgeStatus = view.isHostPresent(node) ? EdgeStatus.down : EdgeStatus.up
                    proposalsToReturn.append(contentsOf: aggregate(src: observer, dst: node, edgeStatus: edgeStatus, ringNumber: ringNumber))
                }
                ringNumber += 1
            }
        }

        return Array(proposalsToReturn)
    }

    /// For testing
    public func getProposalCount() -> Int {
        return proposalCount
    }
}

