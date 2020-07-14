import Foundation
import NIO
import Dispatch

// TODO now we use one event loop for all FDs which may not be all that great
class AdaptiveAccrualFailureDetectorProvider: EdgeFailureDetectorProvider {

    private let messagingClient: MessagingClient
    private let el: EventLoop
    private let selfEndpoint: Endpoint
    private let provider: ActorRefProvider

    init(selfEndpoint: Endpoint, messagingClient: MessagingClient, provider: ActorRefProvider, el: EventLoop) {
        self.selfEndpoint = selfEndpoint
        self.messagingClient = messagingClient
        self.provider = provider
        self.el = el
    }

    func createInstance(subject: Endpoint, signalFailure: @escaping (Endpoint) -> ()) throws -> () -> EventLoopFuture<()> {
        let failureDetectorRef = provider.actorFor(try AdaptiveAccrualFailureDetectorActor(subject: subject, selfEndpoint: selfEndpoint, signalFailure: signalFailure, messagingClient: messagingClient, el: el))
        failureDetectorRef.tell(.initialize(failureDetectorRef))
        return {
            failureDetectorRef.ask(AdaptiveAccrualFailureDetectorActor.FailureDetectorProtocol.tick)
        }
    }

}

class AdaptiveAccrualFailureDetectorActor: Actor {
    typealias MessageType = FailureDetectorProtocol
    typealias ResponseType = Void // TODO Nothing type???

    internal let el: EventLoop
    private let subject: Endpoint
    private let selfEndpoint: Endpoint
    private let signalFailure: (Endpoint) -> ()
    private let messagingClient: MessagingClient
    private let fd: AdaptiveAccrualFailureDetector
    private var hasNotified = false

    private let probeRequest: RapidRequest

    // TODO be less of a troll with the naming
    private var this: ActorRef<AdaptiveAccrualFailureDetectorActor>? = nil

    init(subject: Endpoint, selfEndpoint: Endpoint, signalFailure: @escaping (Endpoint) -> (), messagingClient: MessagingClient, el: EventLoop) throws {
        self.subject = subject
        self.selfEndpoint = selfEndpoint
        self.signalFailure = signalFailure
        self.messagingClient = messagingClient
        self.el = el

        // TODO config from settings
        try self.fd = AdaptiveAccrualFailureDetector(threshold: 0.2, maxSampleSize: 1000, scalingFactor: 0.9, clock: currentTimeNanos)
        self.probeRequest = RapidRequest.with {
            $0.probeMessage = ProbeMessage.with({
                $0.sender = selfEndpoint
            })
        }

    }

    func receive(_ msg: MessageType, _ callback: ((Result<ResponseType, Error>) -> ())?) {
        switch(msg) {
        case .initialize(let ref):
            self.this = ref
        case .tick:
            let now = currentTimeNanos()
            if (!fd.isAvailable(at: now) && !hasNotified) {
                callback?(Result.success(signalFailure(subject)))
                hasNotified = true
            } else {
                let _ = self.messagingClient
                    .sendMessageBestEffort(recipient: subject, msg: self.probeRequest)
                    .map { response in
                        switch(response.content) {
                        case .probeResponse:
                            // TODO handle probe status / fail after too many probes in initializing state
                            self.this?.tell(.heartbeat)
                        default:
                            return
                        }
                    }
                callback?(Result.success(()))
            }
        case .heartbeat:
            fd.heartbeat()
        }
    }


    enum FailureDetectorProtocol {
        case initialize(ActorRef<AdaptiveAccrualFailureDetectorActor>)
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
