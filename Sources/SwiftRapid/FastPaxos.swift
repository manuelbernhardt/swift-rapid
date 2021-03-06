import Foundation
import NIO
import Logging

/// Single-decree Fast Paxos: each node acts both as a proposer and an acceptor, therefore we only have one phase
///
/// Original paper: https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf
/// Generalised Fast Paxos, easier to read: section 5 of https://arxiv.org/pdf/1902.06776.pdf
///
/// This class is not thread safe
///
class FastPaxos {
    private let logger = Logger(label: "rapid.FastPaxos")

    private let fallbackJitterRate: Double
    private let selfEndpoint: Endpoint
    private let configurationId: UInt64
    private let membershipSize: Int
    private var wrappedDecisionCallback: ([Endpoint]) -> () = { _ in () }
    private let messagingClient: MessagingClient
    private let broadcaster: Broadcaster
    private let el: EventLoop
    private let settings: Settings

    private var votesPerProposal = [[Endpoint] : Int]()
    private var receivedVotes = Set<Endpoint>()
    private let paxos: Paxos
    private var scheduledClassicRoundTask: Scheduled<Void>?
    private var decided: Bool = false

    init(selfEndpoint: Endpoint,
         configurationId: UInt64,
         membershipSize: Int,
         decisionCallback: @escaping ([Endpoint])-> (),
         messagingClient: MessagingClient,
         broadcaster: Broadcaster,
         settings: Settings,
         el: EventLoop) {
        self.selfEndpoint = selfEndpoint
        self.configurationId = configurationId
        self.membershipSize = membershipSize
        self.messagingClient = messagingClient
        self.broadcaster = broadcaster
        self.settings = settings
        self.el = el

        // the rate used to determine a jitter over a base delay to fall back to a classic paxos round
        // given that full paxos is rather verbose in terms of messages it is a good idea not to have all nodes start
        // at the same time

        self.fallbackJitterRate = 1 / Double(membershipSize)
        self.paxos = Paxos(selfEndpoint: selfEndpoint, configurationId: configurationId, N: membershipSize, messagingClient: messagingClient, broadcaster: broadcaster, onDecide: wrappedDecisionCallback)
        self.wrappedDecisionCallback = { endpoints in
            self.decided = true
            decisionCallback(endpoints)
            if(self.scheduledClassicRoundTask != nil) {
                self.scheduledClassicRoundTask?.cancel()
                self.scheduledClassicRoundTask = nil
            }
        }
    }

    /// Propose a value for a fast round with a delay to trigger the recovery protocol
    ///
    /// - Parameters:
    ///   - proposal: the membership change proposal consisting of all the nodes that have been added / removed
    ///               in comparison to the previous configuration
    func propose(proposal: [Endpoint]) -> EventLoopFuture<()> {
        return propose(proposal: proposal, fallbackDelay: randomFallbackDelay())
    }

    /// Propose a value for a fast round with a delay to trigger the recovery protocol
    ///
    /// - Parameters:
    ///   - proposal: the membership change proposal consisting of all the nodes that have been added / removed
    ///               in comparison to the previous configuration
    ///   - fallbackDelayInMs: the delay before falling back to classic paxos
    func propose(proposal: [Endpoint], fallbackDelay: TimeAmount) -> EventLoopFuture<()> {
        paxos.registerFastRoundVote(vote: proposal)

        let consensusMessage = FastRoundPhase2bMessage.with {
            $0.configurationID = configurationId
            $0.endpoints = proposal
            $0.sender = selfEndpoint
        }
        let proposalMessage = RapidRequest.with {
            $0.fastRoundPhase2BMessage = consensusMessage
        }

        return broadcaster.broadcast(request: proposalMessage).map { _ in
            ()
        }

        logger.trace("Scheduling classic round with delay: \(fallbackDelay)")

        self.scheduledClassicRoundTask = el.scheduleTask(in: fallbackDelay, {
            self.paxos.startPhase1a(round: 2)
        })

    }

    /// Accepts a proposal message from other nodes for a fast round
    func handleFastRoundProposal(proposalMessage: FastRoundPhase2bMessage) {
        if (proposalMessage.configurationID != configurationId) {
            logger.trace("Configuration ID mismatch for proposal, current configuration: \(configurationId) proposal: \(proposalMessage.configurationID)")
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
                logger.debug("Proposal of size \(proposalMessage.endpoints.count)")
                wrappedDecisionCallback(proposalMessage.endpoints)
            }
        }
    }

    private func randomFallbackDelay() -> TimeAmount {
        let jitter: Double = -1000 * log(1 - Double.random(in: 0..<1)) / fallbackJitterRate
        return TimeAmount.nanoseconds(TimeAmount.milliseconds(Int64(jitter)).nanoseconds + settings.consensusFallbackBaseDelay.nanoseconds)
    }





}
