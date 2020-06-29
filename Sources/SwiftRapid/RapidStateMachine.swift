import Foundation

struct RapidStateMachine {

    private var state: State

    enum State {
        case active(ActiveState)
        case viewChanging(ViewChangingState)
        case leaving
        case left
    }

    /// Initialize the Rapid state machine for an empty cluster (this is the bootstrapping node)
    init(selfEndpoint: Endpoint, settings: Settings, failureDetectorProvider: EdgeFailureDetectorProvider,
         broadcaster: Broadcaster, messagingClient: MessagingClient, selfMetadata: Metadata) {

        let commonState = CommonState(
            selfEndpoint: selfEndpoint,
                settings: settings,
                view: MembershipView(K: settings.K),
                metadata: [selfEndpoint: selfMetadata],
                failureDetectorProvider: failureDetectorProvider,
                broadcaster: broadcaster,
                messagingClient: messagingClient)
        let activeState = ActiveState(
            common: commonState,
            cutDetector: try! MultiNodeCutDetector(K: settings.K, H: settings.H, L: settings.L)
        )
        self.state = .active(activeState)
    }

    /// Initialize the Rapid state machine for an existing cluster that this node is joining
    init(selfEndpoint: Endpoint, settings: Settings, failureDetectorProvider: EdgeFailureDetectorProvider,
                     broadcaster: Broadcaster, messagingClient: MessagingClient, selfMetadata: Metadata,
                     nodeIds: [NodeId], endpoints: [Endpoint], metadata: [Endpoint: Metadata]) {

        let commonState = CommonState(
            selfEndpoint: selfEndpoint,
            settings: settings,
            view: MembershipView(K: settings.K, nodeIds: nodeIds, endpoints: endpoints),
            metadata: metadata,
            failureDetectorProvider: failureDetectorProvider,
            broadcaster: broadcaster,
            messagingClient: messagingClient)
        let activeState = ActiveState(
            common: commonState,
            cutDetector: try! MultiNodeCutDetector(K: settings.K, H: settings.H, L: settings.L)
        )
        self.state = .active(activeState)
    }

    struct ActiveState {
        var common: CommonState

        // ~~~ membership protocol
        var cutDetector: MultiNodeCutDetector
        // TODO var failureDetectors: <some kind of scheduled futures>

        var alertMessageQueue = [AlertMessage]()

        // ~~~ postponed consensus messages
        // we only consider those once we have transitioned to the viewChanging state
        var postponedConsensusMessages = [RapidRequest]()
    }

    struct ViewChangingState {
        var common: CommonState
        var fastPaxos: FastPaxos

        // no more alert message queue, we have flushed them before
        // no need for a cut detector, we already made the decision to reconfigure
        // no active failure detectors
        init(_ previousState: ActiveState) {
            self.common = previousState.common
            self.fastPaxos = FastPaxos(
                selfEndpoint: common.selfEndpoint,
                configurationId: common.view.getCurrentConfigurationId(),
                membershipSize: common.view.getMembershipSize(),
                decisionCallback: { (endpoints: [Endpoint]) in
                    // TODO send message back to ourselves that triggers the state change
                },
                broadcaster: self.common.broadcaster,
                settings: self.common.settings
            )
        }
    }

    struct CommonState {
        var selfEndpoint: Endpoint
        var settings: Settings

        // ~~~ membership protocol
        var view: MembershipView
        var metadata: [Endpoint:Metadata]
        var failureDetectorProvider: EdgeFailureDetectorProvider

        // ~~~ joiner state
        var joiners = [Endpoint:[RapidResponse]]()
        var joinerNodeIds = [Endpoint:NodeId]()
        var joinerMetadata = [Endpoint:Metadata]()

        // ~~~ communication
        var broadcaster: Broadcaster
        var messagingClient: MessagingClient
    }


    // ~~~ event handling

    // TODO not all of these are mutating, some are just side-effecting

    mutating func handleJoin(msg: JoinMessage) throws {
        fatalError("Not implemented")
    }

    mutating func handleAlert(msg: BatchedAlertMessage) throws {
        fatalError("Not implemented")
    }

    mutating func handleProbe(msg: ProbeMessage) throws {
        fatalError("Not implemented")
    }

    mutating func handleLeave(msg: LeaveMessage) throws {
        fatalError("Not implemented")
    }

    mutating func handleConsensus(msg: RapidRequest) throws {
        fatalError("Not implemented")
    }

}
