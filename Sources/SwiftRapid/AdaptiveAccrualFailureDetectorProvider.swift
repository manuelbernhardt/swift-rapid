import Foundation
import NIO
import Dispatch

// FIXME if there has been zero successful probes, we reply that the FD is healthy (to not fail right away)
// FIXME but if this continues for too long then of course this is wrong. we need a mechanism to take this into account
class AdaptiveAccrualFailureDetectorProvider: EdgeFailureDetectorProvider {

    private let messagingClient: MessagingClient
    private let el: EventLoop
    private let selfEndpoint: Endpoint
    private let settings: Settings
    private let provider: ActorRefProvider

    init(selfEndpoint: Endpoint, messagingClient: MessagingClient, provider: ActorRefProvider, settings: Settings, el: EventLoop) {
        self.selfEndpoint = selfEndpoint
        self.messagingClient = messagingClient
        self.provider = provider
        self.settings = settings
        self.el = el
    }

    func createInstance(subject: Endpoint, signalFailure: @escaping (Endpoint) -> ()) throws -> () -> EventLoopFuture<()> {
        let failureDetectorRef = try provider.actorFor { el in
            try AdaptiveAccrualFailureDetectorActor(subject: subject, selfEndpoint: selfEndpoint, signalFailure: signalFailure, expectFirstHeartbeatAfter: Double(settings.failureDetectorInterval.nanoseconds) / 1000000000, messagingClient: messagingClient, el: el)
        }
        try failureDetectorRef.start()
        return {
            failureDetectorRef.ask(AdaptiveAccrualFailureDetectorActor.FailureDetectorProtocol.tick)
        }
    }

}

final class AdaptiveAccrualFailureDetectorActor: Actor {
    typealias MessageType = FailureDetectorProtocol
    typealias ResponseType = Void // TODO Nothing type???

    internal let el: EventLoop
    private let subject: Endpoint
    private let selfEndpoint: Endpoint
    private let signalFailure: (Endpoint) -> ()
    private let messagingClient: MessagingClient
    private let fd: AdaptiveAccrualFailureDetector
    private let expectFirstHeartbeatAfter: Double
    private var hasNotified = false
    private var firstHeartbeatSent = false

    private let probeRequest: RapidRequest

    // TODO be less of a troll with the naming
    private var this: ActorRef<AdaptiveAccrualFailureDetectorActor>? = nil

    init(subject: Endpoint, selfEndpoint: Endpoint, signalFailure: @escaping (Endpoint) -> (), expectFirstHeartbeatAfter: Double, messagingClient: MessagingClient, el: EventLoop) throws {
        self.subject = subject
        self.selfEndpoint = selfEndpoint
        self.signalFailure = signalFailure
        self.messagingClient = messagingClient
        self.expectFirstHeartbeatAfter = expectFirstHeartbeatAfter
        self.el = el

        // TODO config from settings
        try self.fd = AdaptiveAccrualFailureDetector(threshold: 0.2, maxSampleSize: 1000, scalingFactor: 0.9, clock: currentTimeNanos)
        self.probeRequest = RapidRequest.with {
            $0.probeMessage = ProbeMessage.with({
                $0.sender = selfEndpoint
            })
        }
    }

    func start(ref: ActorRef<AdaptiveAccrualFailureDetectorActor>) throws {
        self.this = ref
    }

    func stop(el: EventLoop) -> EventLoopFuture<Void> {
        // not much we have to do here
        el.makeSucceededFuture(())
    }

    let BootstrapLimit = 30
    var bootstrappingCount = 0

    func receive(_ msg: MessageType, _ callback: ((Result<ResponseType, Error>) -> ())?) {
        switch(msg) {
        case .tick:
            let now = currentTimeNanos()
            if(!firstHeartbeatSent) {
                // stabilize inter-arrival time by using a synthetic first one, as connection establishment takes time
                DispatchQueue.main.asyncAfter(deadline: .now() + expectFirstHeartbeatAfter) {
                    self.this?.tell(.heartbeat)
                }
                let _ = self.messagingClient.sendMessageBestEffort(recipient: subject, msg: self.probeRequest).hop(to: el).map { _ in
                    self.firstHeartbeatSent = true
                }
                callback?(Result.success(()))
            } else if (firstHeartbeatSent && !fd.isAvailable(at: now) && !hasNotified) {
                //print("\(selfEndpoint.port): Node \(subject.port) failed with a probability of \(fd.suspicion())")
                callback?(Result.success(signalFailure(subject)))
                hasNotified = true
            } else if(firstHeartbeatSent && !hasNotified) {
                let _ = self.messagingClient
                    .sendMessageBestEffort(recipient: subject, msg: self.probeRequest)
                    .hop(to: el)
                    .map { response in
                        switch(response.content) {
                        case .probeResponse:
                            if(response.probeResponse.status == NodeStatus.bootstrapping && self.bootstrappingCount < self.BootstrapLimit) {
                                self.bootstrappingCount += 1
                                self.this?.tell(.heartbeat)
                            } else if(response.probeResponse.status == NodeStatus.ok) {
                                self.this?.tell(.heartbeat)
                            }
                        default:
                            return
                        }
                    }
                callback?(Result.success(()))
            } else {
                callback?(Result.success(()))
            }
        case .heartbeat:
            fd.heartbeat()
        }
    }


    enum FailureDetectorProtocol {
        case tick
        case heartbeat
    }
}

///  Implementation of 'A New Adaptive Accrual Failure Detector for Dependable Distributed Systems' by Satzger al. as defined in their paper:
///  [https://pdfs.semanticscholar.org/8805/d522cd6cef723aae55595f918e09914e4316.pdf]
///
///  The idea of this failure detector is to predict the arrival time of the next heartbeat based on
///  the history of inter-arrival times between heartbeats. The algorithm approximates the cumulative distribution function (CDF)
///  of inter-arrival times of the heartbeat messages.
///
///  The suspicion value of a failure is calculated as follows:
///
///  ```
///  P = |StΔ| / |S|
///  ```
///
///  where:
///  - S is the list of historical inter-arrival times of heartbeats
///  - StΔ the list of inter-arrival times that are smaller or equal to tΔ
///  - tΔ = previous heartbeat timestamp - current heartbeat timestamp
///
/// This class is not thread-safe
class AdaptiveAccrualFailureDetector {

    private let threshold: Double
    private let maxSampleSize: Int
    private let scalingFactor: Double

    private let clock: () -> UInt64

    private var state = State(intervals: [], freshnessPoint: nil)

    /// Create a new adaptive accrual failure detector instance
    ///
    /// - Parameters:
    ///   - threshold: A low threshold is prone to generate many wrong suspicions but ensures a quick detection in the event
    //                 of a real crash. Conversely, a high threshold generates fewer mistakes but needs more time to detect
    //                 actual crashes
    ///   - maxSampleSize: Number of samples to use for calculation of mean and standard deviation of
    //                     inter-arrival times.
    ///   - scalingFactor: A scaling factor to prevent the failure detector to overestimate the probability of failures
    //                     particularly in the case of increasing network latency times
    ///   - clock: The clock, returning current time in milliseconds, but can be faked for testing
    //             purposes. It is only used for measuring intervals (duration).
    /// - Throws: A ValidityError if the parameters aren't acceptable
    init(threshold: Double, maxSampleSize: Int, scalingFactor: Double, clock: @escaping () -> UInt64) throws {
        self.threshold = threshold
        self.maxSampleSize = maxSampleSize
        self.scalingFactor = scalingFactor
        self.clock = clock

        try require(threshold > 0.0, "Threshold must be strictly positive")
        try require(maxSampleSize > 0, "Max sample size must be strictly positive")
        try require(scalingFactor > 0.0, "Scaling factor must be strictly positive")
    }

    func isAvailable() -> Bool {
        return isAvailable(at: clock())
    }

    func suspicion() -> Double {
        return suspicion(timestamp: clock())
    }

    func isAvailable(at timestamp: UInt64) -> Bool {
        return suspicion(timestamp: timestamp) < threshold
    }

    func heartbeat() {
        let timestamp = clock()
        var newIntervals = [UInt64](state.intervals)

        if let freshnessPoint = state.freshnessPoint {
            let tΔ = timestamp - freshnessPoint
            if (state.intervals.count >= maxSampleSize) {
                newIntervals.removeFirst()
            }
            newIntervals.append(tΔ)
        } else {
            // this is heartbeat from a new resource
            // according to the algorithm do not add any initial history
        }

        let newState = State(intervals: newIntervals, freshnessPoint: timestamp)
        self.state = newState
    }

    private func suspicion(timestamp: UInt64) -> Double {
        guard let freshnessPoint = state.freshnessPoint else {
            // treat unmanaged connections, e.g. without initial state, as healthy connections
            return 0.0
        }

        if (state.intervals.isEmpty) {
            // treat unmanaged connections, e.g. with zero heartbeats, as healthy connections
            return 0.0
        } else {
            let tΔ = timestamp - freshnessPoint
            let S = state.intervals
            let SLength = S.count
            let StΔLength = S.filter { interval in Double(interval) <= Double(tΔ) * scalingFactor }.count

            return Double(StΔLength) / Double(SLength)
        }
    }

    struct State {
        let intervals: [UInt64]
        let freshnessPoint: Optional<UInt64>
    }

}
