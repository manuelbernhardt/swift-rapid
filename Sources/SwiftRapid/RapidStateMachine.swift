import Foundation
import NIO

class RapidStateMachine: Actor {
    typealias MessageType = RapidProtocol
    typealias ResponseType = RapidResponse

    internal let el: EventLoop
    internal let dispatchQueue = DispatchQueue(label: "rapid.serial.queue")

    private var state: State

    enum State {
        case initial(CommonState)
        case active(ActiveState)
        case viewChanging(ViewChangingState)
        case leaving
        case left
    }

    func receive(_ msg: MessageType, _ callback: ((Result<ResponseType, Error>) -> ())? = nil) {
        do {
            switch(msg) {
                case .rapidRequest(let request):
                    switch request.content {
                        case .joinMessage(let join):
                            let response = try handleJoin(msg: join)
                            callback?(Result.success(response))
                        case .batchedAlertMessage(let alert):
                            let response = try handleAlert(msg: alert)
                            callback?(Result.success(response))
                        case .probeMessage(let probe):
                            let response = try handleProbe(msg: probe)
                            callback?(Result.success(response))
                        case .fastRoundPhase2BMessage, .phase1AMessage, .phase1BMessage, .phase2AMessage, .phase2BMessage:
                            let response = try handleConsensus(msg: request)
                            callback?(Result.success(response))
                        case .leaveMessage(let leave):
                            let response = try handleLeave(msg: leave)
                            callback?(Result.success(response))
                        case .none:
                            return
                        }
                case .subjectFailed(let subject):
                    onSubjectFailed(subject)
            }
        } catch {
            callback?(Result.failure(error))
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

        self.state = .initial(commonState)
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

        self.state = .initial(commonState)
    }

    /// Starts the state machine by switching to the active state
    /// TODO: it would be nicer not to have to provide the reference here. maybe there's a way
    func start(ref: ActorRef<RapidStateMachine>) throws {
        switch state {
           case .initial(let commonState):
                let activeState = try ActiveState(commonState, onSubjectFailed: { subject in ref.tell(RapidProtocol.subjectFailed(subject)) }, el: el)
                self.state = .active(activeState)
            default:
                fatalError("Can only start in initial state")
        }
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

        init(_ common: CommonState, onSubjectFailed: @escaping (Endpoint) -> (), el: EventLoop) throws {
            self.common = common
            common.broadcaster.setMembership(recipients: common.view.getRing(k: 0).contents)
            try self.cutDetector = MultiNodeCutDetector(K: common.settings.K, H: common.settings.H, L: common.settings.L)

            // failure detectors
            let subjects = try common.view.getSubjectsOf(node: common.selfEndpoint)
            try self.failureDetectors = subjects.map { subject in
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

    func handleJoin(msg: JoinMessage) throws -> RapidResponse {
        fatalError("Not implemented")
    }

    func handleAlert(msg: BatchedAlertMessage) throws  -> RapidResponse {
        fatalError("Not implemented")
    }

    func handleProbe(msg: ProbeMessage) throws  -> RapidResponse {
        fatalError("Not implemented")
    }

    func handleLeave(msg: LeaveMessage) throws -> RapidResponse {
        fatalError("Not implemented")
    }

    func handleConsensus(msg: RapidRequest) throws -> RapidResponse {
        fatalError("Not implemented")
    }

}
