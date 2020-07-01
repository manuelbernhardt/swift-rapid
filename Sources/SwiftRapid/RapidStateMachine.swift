import Foundation
import NIO

struct RapidStateMachine: Actor {
    typealias MessageType = RapidProtocol
    typealias ResponseType = RapidResponse

    internal let el: EventLoop
    internal let dispatchQueue = DispatchQueue(label: "rapid.serial.queue")

    private var state: State

    enum State {
        case active(ActiveState)
        case viewChanging(ViewChangingState)
        case leaving
        case left
    }

    mutating func receive(_ msg: MessageType, _ callback: ((ResponseType) -> ())? = nil) {
        do {
            switch(msg) {
                case .rapidRequest(let request):
                    switch request.content {
                        case .joinMessage(let join):
                            let response = try handleJoin(msg: join)
                            callback?(response)
                        case .batchedAlertMessage(let alert):
                            let response = try handleAlert(msg: alert)
                            callback?(response)
                        case .probeMessage(let probe):
                            let response = try handleProbe(msg: probe)
                            callback?(response)
                        case .fastRoundPhase2BMessage, .phase1AMessage, .phase1BMessage, .phase2AMessage, .phase2BMessage:
                            let response = try handleConsensus(msg: request)
                            callback?(response)
                        case .leaveMessage(let leave):
                            let response = try handleLeave(msg: leave)
                            callback?(response)
                        case .none:
                            return
                        }
                case .subjectFailed(let subject):
                    onSubjectFailed(subject)
            }
        } catch {
            // TODO error handling - need to change the callback so it may fail
        }
    }

    /// Initialize the Rapid state machine for an empty cluster (this is the bootstrapping node)
    init(selfEndpoint: Endpoint, settings: Settings, failureDetectorProvider: EdgeFailureDetectorProvider,
         broadcaster: Broadcaster, messagingClient: MessagingClient, selfMetadata: Metadata,
         el: EventLoop) throws {

        self.el = el

        let commonState = CommonState(
            selfEndpoint: selfEndpoint,
                settings: settings,
                view: MembershipView(K: settings.K),
                metadata: [selfEndpoint: selfMetadata],
                failureDetectorProvider: failureDetectorProvider,
                broadcaster: broadcaster,
                messagingClient: messagingClient)

        // FIXME OK this is broken
        // FIXME we can't reference this here because it references self which isn't initialized yet
        // so... we could pass in a closure that will reference this - nope
        // ugh
        let activeState = try ActiveState(commonState, { subject in self.this().tell(RapidProtocol.subjectFailed(subject)) }, el)

        self.state = .active(activeState)
    }

    /// Initialize the Rapid state machine for an existing cluster that this node is joining
    /// TODO is there no way to overload the other initializer?
    init(selfEndpoint: Endpoint, settings: Settings, failureDetectorProvider: EdgeFailureDetectorProvider,
         broadcaster: Broadcaster, messagingClient: MessagingClient, selfMetadata: Metadata,
         nodeIds: [NodeId], endpoints: [Endpoint], metadata: [Endpoint: Metadata],
         el: EventLoop) throws {

        self.el = el

        let commonState = CommonState(
            selfEndpoint: selfEndpoint,
            settings: settings,
            view: MembershipView(K: settings.K, nodeIds: nodeIds, endpoints: endpoints),
            metadata: metadata,
            failureDetectorProvider: failureDetectorProvider,
            broadcaster: broadcaster,
            messagingClient: messagingClient)

        let activeState = try ActiveState(commonState, { subject in self.this().tell(RapidProtocol.subjectFailed(subject)) }, el)

        self.state = .active(activeState)
    }

    func onSubjectFailed(_ subject: Endpoint) {
        fatalError("Not implemented")
    }


    /// ~~~ protocol

    enum RapidProtocol {
        case rapidRequest(RapidRequest)
        case subjectFailed(Endpoint)
    }

    /// ~~~ states

    struct ActiveState {
        var common: CommonState

        // ~~~ membership protocol
        var cutDetector: MultiNodeCutDetector
        var failureDetectors: [RepeatedTask]

        var alertMessageQueue = [AlertMessage]()

        // ~~~ postponed consensus messages
        // we only consider those once we have transitioned to the viewChanging state
        var postponedConsensusMessages = [RapidRequest]()

        init(_ common: CommonState, _ onSubjectFailed: @escaping (Endpoint) -> (), _ el: EventLoop) throws {
            self.common = common
            common.broadcaster.setMembership(recipients: common.view.getRing(k: 0).contents)
            try self.cutDetector = MultiNodeCutDetector(K: common.settings.K, H: common.settings.H, L: common.settings.L)

            // failure detectors
            let subjects = try common.view.getSubjectsOf(node: common.selfEndpoint)
            self.failureDetectors = try subjects.map { subject in
                let fd = try common.failureDetectorProvider.createInstance(subject: subject, signalFailure: { failedSubject in
                    onSubjectFailed(failedSubject)
                })
                let fdTask = { (task: RepeatedTask) in
                    fd()
                }
                return el.scheduleRepeatedAsyncTask(initialDelay: TimeAmount.seconds(0), delay: common.settings.failureDetectorInterval, fdTask)
            }

            // TODO initialize batching for outgoing alert messages
        }

        mutating func enqueueAlertMessage(_ msg: AlertMessage) {
            self.alertMessageQueue.append(msg)
        }


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

    mutating func handleJoin(msg: JoinMessage) throws -> RapidResponse {
        fatalError("Not implemented")
    }

    mutating func handleAlert(msg: BatchedAlertMessage) throws  -> RapidResponse {
        fatalError("Not implemented")
    }

    mutating func handleProbe(msg: ProbeMessage) throws  -> RapidResponse {
        fatalError("Not implemented")
    }

    mutating func handleLeave(msg: LeaveMessage) throws -> RapidResponse {
        fatalError("Not implemented")
    }

    mutating func handleConsensus(msg: RapidRequest) throws -> RapidResponse {
        fatalError("Not implemented")
    }

}
