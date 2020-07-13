import Foundation
import NIO
import Dispatch
import Logging

/// TODO
/// - graceful leaving of this node
/// - API and callbacks for membership change
class RapidStateMachine: Actor {
    private let logger = Logger(label: "rapid.RapidStateMachine")

    typealias MessageType = RapidProtocol
    typealias ResponseType = RapidResponse

    internal let el: EventLoop

    private var state: State

    enum State {
        case initial(CommonState)
        case active(ActiveState)
        case viewChanging(ViewChangingState)
        case leaving
        case left
    }

    enum RapidStateMachineError: Error {
        case messageInInvalidState(State)
        case viewChangeInProgress
        case noStateAvailable
    }

    func receive(_ msg: MessageType, _ callback: ((Result<ResponseType, Error>) -> ())? = nil) {
        do {
            switch(state) {
                case .initial:
                    throw RapidStateMachineError.messageInInvalidState(state)
                case .active(var currentState):
                    let nextState = try currentState.handleMessage(msg, callback)
                    self.state = nextState
                case .viewChanging(var currentState):
                    let nextState = try currentState.handleMessage(msg, callback)
                    self.state = nextState
                case .leaving, .left:
                    // TODO we need to do graceful handling of messages in leaving and then transition to left
                    throw RapidStateMachineError.messageInInvalidState(state)
            }
        } catch {
            callback?(Result.failure(error))
        }
    }

    /// Initialize the Rapid state machine
    init(selfEndpoint: Endpoint, settings: Settings, view: MembershipView, failureDetectorProvider: EdgeFailureDetectorProvider,
         broadcaster: Broadcaster, messagingClient: MessagingClient, allMetadata: [Endpoint: Metadata],
         subscriptions: [(RapidCluster.ClusterEvent) -> ()],
         el: EventLoop) throws {

        self.el = el

        let commonState = CommonState(
            selfEndpoint: selfEndpoint,
                settings: settings,
                view: view,
                metadata: allMetadata,
                failureDetectorProvider: failureDetectorProvider,
                broadcaster: broadcaster,
                messagingClient: messagingClient,
                subscriptions: subscriptions,
                el: el)

        self.state = .initial(commonState)
    }

    /// Starts the state machine by switching to the active state
    /// TODO: it would be nicer not to have to provide the reference here. maybe there's a way
    func start(ref: ActorRef<RapidStateMachine>) throws {
        switch state {
           case .initial(let commonState):
               var activeState = try ActiveState(commonState, ref: ref)

               // batch alerts
               activeState.common.alertBatchJob = el.scheduleRepeatedTask(initialDelay: commonState.settings.batchingWindow, delay: commonState.settings.batchingWindow) { _ in
                   ref.tell(.batchedAlertTick)
               }

               self.state = .active(activeState)
           default:
                fatalError("Can only start in initial state")
        }
    }

    func getMemberList() throws -> [Endpoint] {
        try retrieveState { $0.view.getRing(k: 0).contents }
    }

    func getMetadata() throws -> [Endpoint: Metadata] {
        try retrieveState { $0.metadata }
    }

    private func retrieveState<State>(_ retrieve: (CommonState) -> State) throws -> State {
        switch state {
            case .initial, .leaving, .left:
                throw RapidStateMachineError.noStateAvailable
            case .active(let activeState):
                return retrieve(activeState.common)
            case .viewChanging:
                throw RapidStateMachineError.viewChangeInProgress
        }
    }

    @discardableResult
    func shutdown() -> EventLoopFuture<Void> {
        func cancelFailureDetectors(failureDetectors: [RepeatedTask]) -> EventLoopFuture<Void> {
            let cancellationFutures: [EventLoopFuture<Void>] = failureDetectors.map { fd in
                let promise: EventLoopPromise<Void> = el.makePromise()
                fd.cancel(promise: promise)
                return promise.futureResult
            }
            return EventLoopFuture.whenAllComplete(cancellationFutures, on: el).map { _ in ()}
        }
        switch state {
            case .active(let activeState):
                activeState.common.alertBatchJob?.cancel()
                return cancelFailureDetectors(failureDetectors: activeState.failureDetectors)
            case .viewChanging(let viewChangingState):
                viewChangingState.common.alertBatchJob?.cancel()
                return cancelFailureDetectors(failureDetectors: viewChangingState.failureDetectors)
            default:
                return el.makeSucceededFuture(())
        }
    }

    deinit {
        shutdown()
    }

    /// ~~~ protocol

    enum RapidProtocol {
        case rapidRequest(RapidRequest)
        case subjectFailed(Endpoint)
        case viewChangeDecided([Endpoint])
        case batchedAlertTick
    }

    /// ~~~ states

    struct ActiveState: SubjectFailedHandler, ProbeMessageHandler, BatchedAlertMessageHandler, AlertBatcher {
        private let logger = Logger(label: "rapid.RapidStateMachine")

        var common: CommonState
        // TODO be less of a troll with the naming
        var this: ActorRef<RapidStateMachine>

        // ~~~ membership protocol
        var cutDetector: MultiNodeCutDetector
        var failureDetectors: [RepeatedTask]

        // ~~~ postponed consensus messages
        // we only consider those once we have transitioned to the viewChanging state
        var postponedConsensusMessages = [RapidRequest]()

        init(_ common: CommonState, ref: ActorRef<RapidStateMachine>) throws {
            self.common = common
            self.this = ref
            common.broadcaster.setMembership(recipients: common.view.getRing(k: 0).contents)
            try self.cutDetector = MultiNodeCutDetector(K: common.settings.K, H: common.settings.H, L: common.settings.L)

            // failure detectors
            let subjects = try common.view.getSubjectsOf(node: common.selfEndpoint)
            try self.failureDetectors = subjects.map { subject in
                let fd = try common.failureDetectorProvider.createInstance(subject: subject, signalFailure: { failedSubject in
                    ref.tell(.subjectFailed(subject))
                })
                let fdTask = { (task: RepeatedTask) in
                    fd().hop(to: common.el)
                }
                return common.el.scheduleRepeatedAsyncTask(initialDelay: TimeAmount.seconds(0), delay: common.settings.failureDetectorInterval, fdTask)
            }
        }

        init(_ previousState: ViewChangingState) throws {
            try self.init(previousState.common, ref: previousState.this)
        }

        mutating func handleMessage(_ msg: MessageType, _ callback: ((Result<ResponseType, Error>) -> ())? = nil) throws -> State {
            switch(msg) {
                case .rapidRequest(let request):
                    switch request.content {
                        case .joinMessage(let join):
                            // note: we may postpone the reply until the cluster has gone into a new configuration
                            // which is why we pass in the callback here
                            let joinResponse = try handleJoin(msg: join, callback: callback)
                            if let response = joinResponse {
                                let rapidResponse = RapidResponse.with {
                                    $0.joinResponse = response
                                }
                                callback?(Result.success(rapidResponse))
                            }
                            return .active(self)
                        case .batchedAlertMessage(let batchedAlertMessage):
                            let proposal = handleBatchedAlert(msg: batchedAlertMessage)
                            if (proposal.isEmpty) {
                                callback?(Result.success(RapidResponse()))
                                return .active(self)
                            } else {
                                callback?(Result.success(RapidResponse()))
                                return try .viewChanging(ViewChangingState(self, proposal: proposal))
                            }
                        case .probeMessage(let probe):
                            let response = handleProbe(msg: probe)
                            callback?(Result.success(response))
                            return .active(self)
                        case .fastRoundPhase2BMessage, .phase1AMessage, .phase1BMessage, .phase2AMessage, .phase2BMessage:
                            let response = try handleConsensus(msg: request)
                            callback?(Result.success(response))
                            return .active(self)
                        case .leaveMessage(let leave):
                            let response = try handleLeave(msg: leave)
                            callback?(Result.success(response))
                            return .active(self)
                        case .none:
                            return .active(self)
                    }
                case .subjectFailed(let subject):
                    try handleSubjectFailed(subject)
                    return .active(self)
                case .batchedAlertTick:
                    sendAlertBatch()
                    return .active(self)
                case .viewChangeDecided:
                    fatalError("How on earth are we here?")
            }
        }

        func applyCutDetection(alerts: [AlertMessage]) -> [Endpoint] {
            // apply all the valid messages to the cut detector to obtain a view change proposal
            var proposal = alerts.flatMap { cutDetector.aggregate(alert: $0) }

            // apply implicit detections
            proposal.append(contentsOf: cutDetector.invalidateFailingEdges(view: common.view))

            return proposal
        }

        // ~~~ event handling

        private mutating func handleJoin(msg: JoinMessage, callback: ((Result<ResponseType, Error>) -> ())?) throws -> JoinResponse? {
            let configuration = common.view.getCurrentConfiguration()
            let statusCode = common.view.isSafeToJoin(node: msg.sender, uuid: msg.nodeID)
            switch(statusCode) {
            case .sameNodeAlreadyInRing:
                // this can happen if a join attempt times out at the joining node
                // yet the response was about to be sent
                // simply reply that they're welcome to join so they can get the membership list
                let response = JoinResponse.with {
                    $0.sender = common.selfEndpoint
                    $0.configurationID = configuration.configurationId
                    $0.statusCode = JoinStatusCode.safeToJoin
                    $0.endpoints = configuration.endpoints
                    $0.identifiers = Array(configuration.nodeIds)
                    $0.metadataKeys = Array(common.metadata.keys)
                    $0.metadataValues = Array(common.metadata.values)
                }
                return response
            case .safeToJoin:
                common.joiners.append(callback)
                // simulate K alerts, one for each of the expected observers
                let observers = common.view.getExpectedObserversOf(node: msg.sender)
                for i in 0..<observers.count {
                    let joinAlert = AlertMessage.with {
                        $0.edgeSrc = observers[i]
                        $0.edgeDst = msg.sender
                        $0.edgeStatus = EdgeStatus.up
                        $0.configurationID = configuration.configurationId
                        $0.nodeID = msg.nodeID
                        $0.ringNumber = [Int32(i)]
                        $0.metadata = msg.metadata
                    }
                    enqueueAlertMessage(joinAlert)
                }
                return nil
            case .hostnameAlreadyInRing:
                // do not let the node join. it will have to wait until failure detection kicks in and
                // a new membership view is agreed upon in order to be able to join again.
                let response = JoinResponse.with {
                    $0.sender = common.selfEndpoint
                    $0.statusCode = JoinStatusCode.hostnameAlreadyInRing
                }
                return response
            default:
                // in all other cases, the client should try rejoining
                let response = JoinResponse.with {
                    $0.sender = common.selfEndpoint
                    $0.statusCode = statusCode
                }
                return response
            }
        }

        private func handleAlert(msg: BatchedAlertMessage) throws -> RapidResponse {
            fatalError("Not implemented")
        }

        private mutating func handleLeave(msg: LeaveMessage) throws -> RapidResponse {
            // propagate the intent of a node to leave by proactively notifying of edge failure
            try edgeFailureNotification(subject: msg.sender, configurationId: common.view.getCurrentConfigurationId())
            return RapidResponse()
        }

        private mutating func handleConsensus(msg: RapidRequest) throws -> RapidResponse {
            postponedConsensusMessages.append(msg)
            // reply now so that the sender doesn't retry propagating this (in case of an eager broadcasting mechanism)
            return RapidResponse()
        }

    }

    struct ViewChangingState: SubjectFailedHandler, ProbeMessageHandler, BatchedAlertMessageHandler, AlertBatcher {
        var common: CommonState
        // TODO be less of a troll with the naming
        var this: ActorRef<RapidStateMachine>

        var failureDetectors: [RepeatedTask]
        var fastPaxos: FastPaxos

        // a stash of all messages that we didn't want to handle in this state
        var stashedMessages: [RapidRequest] = []

        // no more alert message queue, we have flushed them before
        // no need for a cut detector, we already made the decision to reconfigure
        // no active failure detectors
        init(_ previousState: ActiveState, proposal: [Endpoint]) throws {
            self.common = previousState.common
            self.this = previousState.this
            self.failureDetectors = previousState.failureDetectors
            self.fastPaxos = FastPaxos(
                selfEndpoint: common.selfEndpoint,
                configurationId: common.view.getCurrentConfigurationId(),
                membershipSize: common.view.getMembershipSize(),
                decisionCallback: { (endpoints: [Endpoint]) in
                    previousState.this.tell(.viewChangeDecided(endpoints))
                },
                broadcaster: self.common.broadcaster,
                settings: self.common.settings
            )

            // we just got a proposal, directly start the consensus process on it
            // sort it first with the same seed so that we have a stable proposal on all nodes
            let seed = 0
            var sortedProposal = proposal
            sortedProposal.sort { $0.ringHash(seed: seed) < $1.ringHash(seed: seed) }
            for callback in previousState.common.subscriptions {
                callback(.viewChangeProposal(proposal))
            }
            fastPaxos.propose(proposal: sortedProposal)

            // now directly handle with all the postponed consensus messages we have received while in the active state
            for consensusMessage in previousState.postponedConsensusMessages {
                let _ = try handleConsensus(msg: consensusMessage)
            }
        }

        mutating func handleMessage(_ msg: RapidProtocol, _ callback: ((Result<ResponseType, Error>) -> ())? = nil) throws -> State {
            switch(msg) {
            case .rapidRequest(let request):
                switch request.content {
                case .joinMessage:
                    let response = RapidResponse.with {
                        $0.joinResponse = JoinResponse.with {
                            $0.sender = common.selfEndpoint
                            $0.statusCode = JoinStatusCode.viewChangeInProgress
                        }
                    }
                    callback?(Result.success(response))
                    return .viewChanging(self)
                case .batchedAlertMessage(let batchedAlertMessage):
                    // we don't care about any resulting proposal here as we do not perform cut detection anyway
                    // however, we need to handle the alerts for joining nodes for which we store UUIDs and metadata
                    let _ = handleBatchedAlert(msg: batchedAlertMessage)
                    callback?(Result.success(RapidResponse()))
                    return .viewChanging(self)
                case .probeMessage(let probe):
                    let response = handleProbe(msg: probe)
                    callback?(Result.success(response))
                    return .viewChanging(self)
                case .fastRoundPhase2BMessage, .phase1AMessage, .phase1BMessage, .phase2AMessage, .phase2BMessage:
                    let response = try handleConsensus(msg: request)
                    callback?(Result.success(response))
                    return .viewChanging(self)
                case .leaveMessage:
                    stashedMessages.append(request)
                    // reply immediately, leave is a best-effort operation anyway
                    callback?(Result.success(RapidResponse()))
                    return .viewChanging(self)
                case .none:
                    return .viewChanging(self)
                }
            case .subjectFailed(let subject):
                // we have to handle this message in this state as the failure detectors can trigger right before
                // we switch to this state or whilst we haven't reached consensus yet and the failure detectors are
                // still active. there's a risk that we alert of failed edges with the old configuration ID, which means
                // that the alerts will be ignored by nodes that have already moved to a new configuration or by nodes
                // that have already moved to this state (since we don't apply cut detection while in this state)
                // in this case the failure detection may take a bit longer - but that's just something we need to live
                // with according to the virtual synchrony model
                try handleSubjectFailed(subject)
                return .viewChanging(self)
            case .viewChangeDecided(let proposal):
                try handleViewChangeDecided(proposal: proposal)
                // unstash all and return to active state
                for msg in stashedMessages {
                    self.this.tell(.rapidRequest(msg))
                }
                return .active(try ActiveState(self))
            case .batchedAlertTick:
                sendAlertBatch()
                return .viewChanging(self)
            }
        }

        private func handleConsensus(msg: RapidRequest) throws -> RapidResponse {
            switch(msg.content) {
                case .fastRoundPhase2BMessage:
                    fastPaxos.handleFastRoundProposal(proposalMessage: msg.fastRoundPhase2BMessage)
                    return RapidResponse()
            case .phase1AMessage, .phase1BMessage, .phase2AMessage, .phase2BMessage:
                fatalError("Not implemented")
            default:
                fatalError("Should not be here")
            }
        }

        private mutating func handleViewChangeDecided(proposal: [Endpoint]) throws {
            // disable our failure detectors as they will be recreated when transitioning back into the active state
            for fd in failureDetectors {
                fd.cancel()
            }

            // add or remove nodes to / from the ring
            var statusChanges = [RapidCluster.NodeStatusChange]()

            for node in proposal {
                let isPresent = common.view.isHostPresent(node)
                // if the node is already in the ring, remove it. Else, add it.
                if (isPresent) {
                    try common.view.ringDelete(node: node)
                    let metadata = common.metadata.removeValue(forKey: node)
                    statusChanges.append(RapidCluster.NodeStatusChange(node: node, status: .down, metadata: metadata ?? Metadata()))
                } else {
                    guard let joinerNodeId = common.joinerNodeIds.removeValue(forKey: node) else {
                        // FIXME this will fail if the broadcasted alert about the JOIN and the consensus votes are not
                        // FIXME delivered in order (see 10k node experiment)
                        fatalError("NodeId of joining node unknown")
                    }
                    try common.view.ringAdd(node: node, nodeId: joinerNodeId)
                    guard let joinerMetadata = common.joinerMetadata.removeValue(forKey: node) else {
                        // this shouldn't happen, we should've failed at the UUID missing
                        fatalError("Metadata of joining node unknown")
                    }
                    common.metadata[node] = joinerMetadata
                    statusChanges.append(RapidCluster.NodeStatusChange(node: node, status: .up, metadata: joinerMetadata))
                }
            }

            // respond to the nodes that joined through us
            respondToJoiners(proposal: proposal)

            for callback in common.subscriptions {
                callback(.viewChange(RapidCluster.ViewChange(configurationId: common.view.getCurrentConfigurationId(), statusChanges: statusChanges)))
            }
        }

        func applyCutDetection(alerts: [AlertMessage]) -> [Endpoint] {
            // we already have a proposal in progress so we don't apply cut detection
            []
        }

        mutating func respondToJoiners(proposal: [Endpoint]) {
            let configuration = common.view.getCurrentConfiguration()
            let response = RapidResponse.with {
                $0.joinResponse = JoinResponse.with {
                    $0.sender = common.selfEndpoint
                    $0.statusCode = JoinStatusCode.safeToJoin
                    $0.configurationID = configuration.configurationId
                    $0.endpoints = configuration.endpoints
                    $0.identifiers = Array(configuration.nodeIds)
                    $0.metadataKeys = Array(common.metadata.keys)
                    $0.metadataValues = Array(common.metadata.values)
                }
            }
            for joinerCallback in common.joiners {
                joinerCallback?(Result.success(response))
            }
            common.joiners = []
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
        var joiners = [((Result<ResponseType, Error>) -> ())?]()
        var joinerNodeIds = [Endpoint:NodeId]()
        var joinerMetadata = [Endpoint:Metadata]()

        // ~~~ communication
        var broadcaster: Broadcaster
        var messagingClient: MessagingClient
        var alertMessageQueue = [AlertMessage]()
        var alertSendingDeadline: NIODeadline = NIODeadline.now()
        var alertBatchJob: RepeatedTask?

        var subscriptions: [(RapidCluster.ClusterEvent) -> ()]

        var el: EventLoop
    }

}

protocol SubjectFailedHandler {
    var common: RapidStateMachine.CommonState { get set }

}
extension SubjectFailedHandler where Self: AlertBatcher {
    mutating func handleSubjectFailed(_ subject: Endpoint) throws {
        try edgeFailureNotification(subject: subject, configurationId: common.view.getCurrentConfigurationId())
    }

    mutating func edgeFailureNotification(subject: Endpoint, configurationId: UInt64) throws {
        if (configurationId != common.view.getCurrentConfigurationId()) {
            // TODO INFO log
            print("Ignoring failure notification from old configuration \(configurationId)")
            return
        }
        let alert = try AlertMessage.with {
            $0.edgeSrc = common.selfEndpoint
            $0.edgeDst = subject
            $0.edgeStatus = EdgeStatus.down
            $0.configurationID = configurationId
            try $0.ringNumber = common.view.getRingNumbers(observer: common.selfEndpoint, subject: subject)
        }
        enqueueAlertMessage(alert)
    }

}

protocol ProbeMessageHandler { }
extension ProbeMessageHandler {
    func handleProbe(msg: ProbeMessage) -> RapidResponse {
        RapidResponse.with {
            $0.probeResponse = ProbeResponse()
        }
    }
}

protocol BatchedAlertMessageHandler {
    var common: RapidStateMachine.CommonState { get set }

    func applyCutDetection(alerts: [AlertMessage]) -> [Endpoint]
}
extension BatchedAlertMessageHandler {
    mutating func handleBatchedAlert(msg: BatchedAlertMessage) -> [Endpoint] {
        let validAlerts = msg.messages
                // First, we filter out invalid messages that violate membership invariants.
                .filter(filterAlertMessages)
                // For valid UP alerts, extract the joiner details (UUID and metadata) which is going to be needed
                // when the node is added to the rings
                .map { extractJoinerNodeIdAndMetadata($0) }

        return applyCutDetection(alerts: validAlerts)
    }

    private func filterAlertMessages(alert: AlertMessage) -> Bool {
        let destination = alert.edgeDst
        let currentConfigurationID = common.view.getCurrentConfigurationId()
        if (currentConfigurationID != alert.configurationID) {
            // TODO use logging (TRACE)
            print("AlertMessage for configuration \(alert.configurationID) received during configuration \(currentConfigurationID)")
            return false
        }

        // The invariant we want to maintain is that a node can only go into the
        // membership set once and leave it once.
        if (alert.edgeStatus == EdgeStatus.up && common.view.isHostPresent(destination)) {
            // TODO use logging (TRACE)
            print("AlertMessage with status UP received for node \(destination) already in configuration \(currentConfigurationID)")
            return false
        }
        if (alert.edgeStatus == EdgeStatus.down && !common.view.isHostPresent(destination)) {
            // TODO use logging (TRACE)
            print("AlertMessage with status DOWN received for node \(destination) not in configuration \(currentConfigurationID)")
            return false
        }
        return true
    }

    private mutating func extractJoinerNodeIdAndMetadata(_ alert: AlertMessage) -> AlertMessage {
        if (alert.edgeStatus == EdgeStatus.up) {
            common.joinerNodeIds[alert.edgeDst] = alert.nodeID
            common.joinerMetadata[alert.edgeDst] = alert.metadata
        }
        return alert
    }

}

protocol AlertBatcher {
    var common: RapidStateMachine.CommonState { get set }
}
extension AlertBatcher {

    mutating func enqueueAlertMessage(_ msg: AlertMessage) {
        common.alertMessageQueue.append(msg)
        common.alertSendingDeadline = NIODeadline.uptimeNanoseconds(UInt64(common.settings.batchingWindow.nanoseconds))
    }

    mutating func sendAlertBatch() {
        if (common.alertSendingDeadline.isOverdue()) {
            let batchedAlertRequest = RapidRequest.with {
                $0.batchedAlertMessage = BatchedAlertMessage.with {
                    $0.sender = common.selfEndpoint
                    $0.messages = common.alertMessageQueue
                }
            }
            common.broadcaster.broadcast(request: batchedAlertRequest)
            common.alertMessageQueue = []
        }
    }

}