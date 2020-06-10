import Foundation

/// Single-decree Fast Paxos: each node acts both as a proposer and an acceptor, therefore we only have one phase
///
/// Original paper: https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf
/// Generalised Fast Paxos, easier to read: section 5 of https://arxiv.org/pdf/1902.06776.pdf
///
/// This class is not thread safe
///
/// TODO implement fall back to classic Paxos
class FastPaxos {

    private let fallbackJitterRate: Double
    private let selfEndpoint: Endpoint
    private let configurationId: UInt64
    private let membershipSize: Int
    private var wrappedDecisionCallback: ([Endpoint]) -> () = { _ in () }
    private let broadcaster: Broadcaster
    private let settings: Settings

    private var votesPerProposal = [[Endpoint] : Int]()
    private var receivedVotes = Set<Endpoint>()
    private var decided: Bool = false

    init(selfEndpoint: Endpoint,
         configurationId: UInt64,
         membershipSize: Int,
         decisionCallback: @escaping ([Endpoint])-> (),
         broadcaster: Broadcaster,
         settings: Settings) {
        self.selfEndpoint = selfEndpoint
        self.configurationId = configurationId
        self.membershipSize = membershipSize
        self.broadcaster = broadcaster
        self.settings = settings

        // the rate used to determine a jitter over a base delay to fall back to a classic paxos round
        // given that full paxos is rather verbose in terms of messages it is a good idea not to have all nodes start
        // at the same time

        self.fallbackJitterRate = 1 / Double(membershipSize)
        self.wrappedDecisionCallback = { endpoints in
            self.decided = true
            decisionCallback(endpoints)
        }
    }

    /// Propose a value for a fast round with a delay to trigger the recovery protocol
    ///
    /// - Parameters:
    ///   - proposal: the membership change proposal consisting of all the nodes that have been added / removed
    ///               in comparison to the previous configuration
    func propose(proposal: [Endpoint]) {
        return propose(proposal: proposal, fallbackDelayInMs: randomFallbackDelayInMs())
    }

    /// Propose a value for a fast round with a delay to trigger the recovery protocol
    ///
    /// - Parameters:
    ///   - proposal: the membership change proposal consisting of all the nodes that have been added / removed
    ///               in comparison to the previous configuration
    ///   - fallbackDelayInMs: the delay before falling back to classic paxos
    func propose(proposal: [Endpoint], fallbackDelayInMs: Int) {
        // TODO inform classic paxos we're running a fast round when implementing paxos

        let consensusMessage = FastRoundPhase2bMessage.with {
            $0.configurationID = configurationId
            $0.endpoints = proposal
            $0.sender = selfEndpoint
        }
        let proposalMessage = RapidRequest.with {
            $0.fastRoundPhase2BMessage = consensusMessage
        }
        broadcaster.broadcast(request: proposalMessage)

        // TODO schedule classic round after fallback delay
    }

    /// Accepts a proposal message from other nodes for a fast round
    func handleFastRoundProposal(proposalMessage: FastRoundPhase2bMessage) {
        if (proposalMessage.configurationID != configurationId) {
            // TODO hey we need logging
            print("Configuration ID mismatch for proposal, current configuration: \(configurationId) proposal: \(proposalMessage.configurationID)")
            return
        }

        if (receivedVotes.contains(proposalMessage.sender)) {
            // discard duplicate
            return
        }

        if (decided) {
            // nothing to do here
            return
        }
        receivedVotes.insert(proposalMessage.sender)

        let votesReceivedForProposal = votesPerProposal[proposalMessage.endpoints, default: 0]
        let count = votesReceivedForProposal + 1
        votesPerProposal[proposalMessage.endpoints] = count

        // fast paxos resiliency
        // TODO would be nice to be able to write this in one expression, feels somewhat clunky
        var resiliency = (Double(membershipSize - 1) / Double(4))
        resiliency.round(.down)
        let F = Int(resiliency)

        if (receivedVotes.count >= membershipSize - F) {
            if (count >= membershipSize - F) {
                wrappedDecisionCallback(proposalMessage.endpoints)
            }
        }

    }

    private func randomFallbackDelayInMs() -> Int {
        let jitter = -1000 * log(1 - Double.random(in: 0..<1)) / fallbackJitterRate
        return Int(jitter) + settings.ConsensusFallbackBaseDelayInMs
    }





}
