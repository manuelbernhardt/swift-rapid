import Foundation
import Logging

/**
 *  Single-decree consensus. Implements classic Paxos with the modified rule for the coordinator to pick values as per
 *  the Fast Paxos paper: https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf
 *
 *  The code below assumes that the first round in a consensus instance (done per configuration change) is the
 *  only round that is a fast round. A round is identified by a tuple (rnd-number, nodeId), where nodeId is a unique
 *  identifier per node that initiates phase1.
 */
class Paxos {
    private let logger = Logger(label: "rapid.Paxos")

    private let broadcaster: Broadcaster
    private let messagingClient: MessagingClient
    private let configurationId: UInt64
    private let selfEndpoint: Endpoint
    private let N: Int

    private var rnd: Rank
    private var vrnd: Rank
    private var vval: [Endpoint]
    private var phase1bMessages = [Phase1bMessage]()
    private var acceptResponses = [Rank: [Endpoint: Phase2bMessage]]()

    private var crnd: Rank
    private var cval: [Endpoint]

    private let onDecide: ([Endpoint]) -> ()
    private var decided = false

    init(selfEndpoint: Endpoint, configurationId: UInt64, N: Int, messagingClient: MessagingClient, broadcaster: Broadcaster,
              onDecide: @escaping ([Endpoint]) -> ()) {
        self.broadcaster = broadcaster
        self.messagingClient = messagingClient
        self.configurationId = configurationId
        self.selfEndpoint = selfEndpoint
        self.N = N
        self.onDecide = onDecide

        self.crnd = Rank.with {
            $0.round = 0
            $0.nodeIndex = 0
        }
        self.rnd = Rank.with {
            $0.round = 0
            $0.nodeIndex = 0
        }
        self.vrnd = Rank.with {
            $0.round = 0
            $0.nodeIndex = 0
        }
        self.vval = []
        self.cval = []
    }

    ///
    /// At coordinator, start a classic round. We ensure that even if round numbers are not unique, the
    //  "rank" = (round, nodeId) is unique by using unique node IDs.
    //
    /// - Parameter round: The round number to initiate
    func startPhase1a(round: Int32) {
        if (crnd.round > round) {
            return
        }
        crnd.round = round
        // FIXME aaaah
        crnd.nodeIndex = Int32(selfEndpoint.ringHash(seed: 0))
        logger.debug("Prepare called by \(selfEndpoint) for round \(crnd)")
        let prepare = Phase1aMessage.with {
            $0.configurationID = configurationId
            $0.sender = selfEndpoint
            $0.rank = crnd
        }
        let request = RapidRequest.with {
            $0.phase1AMessage = prepare
        }
        logger.trace("Broadcasting startPhase1a message: \(request)")
        broadcaster.broadcast(request: request)
    }

    ///
    /// At acceptor, handle a phase1a message from a coordinator.
    ///
    /// - Parameter phase1aMessage: phase1a message from a coordinator
    func handlePhase1aMessage(phase1aMessage: Phase1aMessage) {
        if(phase1aMessage.configurationID != configurationId) {
            return
        }

        if (compareRanks(rnd, phase1aMessage.rank) < 0) {
            rnd = phase1aMessage.rank
        } else {
            logger.trace("Rejecting prepareMessage from lower rank: (\(rnd)) (\(phase1aMessage)")
            return
        }

        logger.trace("Sendin back vval:\(vval) vrnd:\(vrnd)")
        let phase1bMessage = Phase1bMessage.with {
            $0.configurationID = configurationId
            $0.rnd = rnd
            $0.sender = selfEndpoint
            $0.vrnd = vrnd
            $0.vval = vval
        }
        let request = RapidRequest.with { $0.phase1BMessage = phase1bMessage }
        let _ = messagingClient.sendMessage(recipient: phase1aMessage.sender, msg: request)
    }

    ///
    /// At coordinator, collect phase1b messages from acceptors to learn whether they have already voted for
    /// any values, and if a value might have been chosen.
    ///
    /// - Parameter phase1bMessage: response messages from acceptors
    func handlePhase1bMessage(phase1bMessage: Phase1bMessage) {
        if (phase1bMessage.configurationID != configurationId) {
            return
        }

        // Only handle responses from crnd == i
        if (compareRanks(crnd, phase1bMessage.rnd) != 0) {
            return
        }

        logger.trace("Handling PrepareResponse: \(phase1bMessage)")

        phase1bMessages.append(phase1bMessage)

        if (phase1bMessages.count > (N / 2)) {
            // selectProposalUsingCoordinator rule may execute multiple times with each additional phase1bMessage
            // being received, but we can enter the following if statement only once when a valid cval is identified.
            let chosenProposal: [Endpoint] = selectProposalUsingCoordinatorRule(phase1bMessages)

            if(crnd.isEqualTo(message: phase1bMessage.rnd) && cval.isEmpty && !chosenProposal.isEmpty) {
                logger.debug("\(selfEndpoint) is proposing: \(chosenProposal) in round \(crnd)")
                cval = chosenProposal
                let phase2aMessage = Phase2aMessage.with {
                    $0.sender = selfEndpoint
                    $0.configurationID = configurationId
                    $0.rnd = crnd
                    vval = chosenProposal
                }
                let request = RapidRequest.with { $0.phase2AMessage = phase2aMessage }
                broadcaster.broadcast(request: request)
            }

        }
    }

    ///
    /// At acceptor, handle an accept message from a coordinator.
    ///
    /// - Parameter phase2aMessage: accept message from coordinator
    func handlePhase2aMessage(phase2aMessage: Phase2aMessage) {
        if (phase2aMessage.configurationID != configurationId) {
            return
        }
        logger.trace("At acceptor received phase2aMessage: \(phase2aMessage)")
        if (compareRanks(rnd, phase2aMessage.rnd) <= 0 && !vrnd.isEqualTo(message: phase2aMessage.rnd)) {
            rnd = phase2aMessage.rnd
            vrnd = phase2aMessage.rnd
            vval = phase2aMessage.vval
            logger.trace("\(selfEndpoint) accepted value in vrnd: \(vrnd), vval: \(vval)")
            let phase2bMessage = Phase2bMessage.with {
                $0.sender = selfEndpoint
                $0.configurationID = configurationId
                $0.rnd = phase2aMessage.rnd
                $0.endpoints = vval
            }
            let request = RapidRequest.with { $0.phase2BMessage = phase2bMessage }
            broadcaster.broadcast(request: request)
        }
    }

    ///
    /// At acceptor, learn about another acceptor's vote (phase2b messages).
    ///
    /// - Parameter phase2bMessage: acceptor's vote
    func handlePhase2bMessage(phase2bMessage: Phase2bMessage) {
        if (phase2bMessage.configurationID != configurationId) {
            return
        }
        logger.trace("Received phase2bMessage: \(phase2bMessage.sender)")

        var phase2bMessagesInRnd = acceptResponses[phase2bMessage.rnd, default: [:]]
        phase2bMessagesInRnd[phase2bMessage.sender] = phase2bMessage
        acceptResponses[phase2bMessage.rnd] = phase2bMessagesInRnd

        if (phase2bMessagesInRnd.count > (N / 2) && !decided) {
            let decision = phase2bMessage.endpoints
            logger.debug("\(selfEndpoint) decided on: \(decision) for round \(phase2bMessage.rnd) \(phase2bMessagesInRnd.count)")
            onDecide(decision)
            decided = true
        }
    }

    ///
    /// This is how we're notified that a fast round is initiated. Invoked by a FastPaxos instance. This
    /// represents the logic at an acceptor receiving a phase2a message directly.
    ///
    /// - Parameter vote: the vote from the fast round
    func registerFastRoundVote(vote: [Endpoint]) {
        // Do not participate in our only fast round if we are already participating in a classic round.
        if (rnd.round > 1) {
            return
        }
        // This is the 1st round in the consensus instance, is always a fast round, and is always the *only* fast round.
        // If this round does not succeed and we fallback to a classic round, we start with round number 2
        // and each node sets its node-index as the hash of its hostname. Doing so ensures that all classic
        // rounds initiated by any host is higher than the fast round, and there is an ordering between rounds
        // initiated by different endpoints.
        rnd.round = 1
        rnd.nodeIndex = 1
        vrnd = rnd
        vval = vote
        logger.trace("Voted in fast round for proposal: \(vote)")
    }

    ///
    /// The rule with which a coordinator picks a value to propose based on the received phase1b messages.
    /// This corresponds to the logic in Figure 2 of the Fast Paxos paper:
    /// https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf
    ///
    /// @param phase1bMessages A list of phase1b messages from acceptors.
    /// @return a proposal to apply
    func selectProposalUsingCoordinatorRule(_ phase1bMessages: [Phase1bMessage]) -> [Endpoint] {
        let minRank = Rank.with {
            $0.round = Int32.min
            $0.nodeIndex = Int32.min
        }
        let maxVrndSoFar: Rank = phase1bMessages
                .map { $0.vrnd }
                .max(by: { compareRanks($0, $1) < 0 }) ?? minRank

        // Let k be the largest value of vr(a) for all a in Q.
        // V (collectedVvals) be the set of all vv(a) for all a in Q s.t vr(a) == k
        let collectedVVals: [[Endpoint]] = phase1bMessages
                .filter { $0.vrnd.isEqualTo(message: maxVrndSoFar) }
                .filter { $0.vval.count > 0 }
                .map { $0.vval }

        let setOfCollectedVval: Set<[Endpoint]> = Set(collectedVVals)
        var chosenProposal: [Endpoint]? = nil

        // If V has a single element, then choose v.
        if (setOfCollectedVval.count == 1) {
            chosenProposal = collectedVVals[0]
        } else if(collectedVVals.count < 1) {
            // if i-quorum Q of acceptors respond, and there is a k-quorum R such that vrnd = k and vval = v,
            // for all a in intersection(R, Q) -> then choose "v". When choosing E = N/4 and F = N/2, then
            // R intersection Q is N/4 -- meaning if there are more than N/4 identical votes.
            var counters = [[Endpoint]: Int]()
            for value in collectedVVals {
                if (counters[value] == nil) {
                    counters[value] = 0
                }
                let count = counters[value]!
                if (count + 1 > (N / 4)) {
                    chosenProposal = value
                    break
                } else {
                    counters[value] = count + 1
                }
            }
        }

        // At this point, no value has been selected yet and it is safe for the coordinator to pick any proposed value.
        // If none of the 'vvals' contain valid values (are all empty lists), then this method returns an empty
        // list. This can happen because a quorum of acceptors that did not vote in prior rounds may have responded
        // to the coordinator first. This is safe to do here for two reasons:
        //      1) The coordinator will only proceed with phase 2 if it has a valid vote.
        //      2) It is likely that the coordinator (itself being an acceptor) is the only one with a valid vval,
        //         and has not heard a Phase1bMessage from itself yet. Once that arrives, phase1b will be triggered
        //         again.
        //
        if (chosenProposal == nil) {
            chosenProposal = phase1bMessages
                    .filter { $0.vval.count > 0 }
                    .map { $0.vval }
                    .first ?? []
            logger.trace("Proposing new value -- chosen:\(chosenProposal), list:\(collectedVVals), vrnd:\(maxVrndSoFar)")
        }

        return chosenProposal ?? []
    }



    ///
    /// Primary ordering is by round number, and secondary ordering by the ID of the node that initiated the round.
    private func compareRanks(_ left: Rank, _ right: Rank) -> Int32 {
        func compare(x: Int32, y: Int32) -> Int32 {
            if (left.round < right.round) {
                return -1
            }
            if (left.round > right.round) {
                return 1
            }

            return 0
        }

        let roundComparison = compare(x: left.round, y: right.round)
        if (roundComparison == 0) {
            return compare(x: left.nodeIndex, y: right.nodeIndex)
        }
        return roundComparison
    }


}
