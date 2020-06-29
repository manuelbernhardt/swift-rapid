import Foundation
import NIO
import NIOConcurrencyHelpers

class AdaptiveAccrualFailureDetectorProvider: EdgeFailureDetectorProvider {

    private let messagingClient: MessagingClient
    private let el: EventLoop
    private let selfAddress: Endpoint

    private let probeRequest: RapidRequest

    init(selfAddress: Endpoint, messagingClient: MessagingClient, el: EventLoop) {
        self.selfAddress = selfAddress
        self.messagingClient = messagingClient
        self.el = el
        self.probeRequest = RapidRequest.with {
            $0.probeMessage = ProbeMessage.with({
                $0.sender = selfAddress
            })
        }
    }

    func createInstance(subject: Endpoint, signalFailure: @escaping () -> EventLoopFuture<()>) throws -> () -> EventLoopFuture<()> {

        // TODO read from settings
        let fd = try AdaptiveAccrualFailureDetector(threshold: 0.2, maxSampleSize: 1000, scalingFactor: 0.9, clock: currentTimeNanos)

        // TODO review the event loop gymnastics here. the aim is to run fd.isAvailable() and fd.heartbeat() from the same thread
        func run() -> EventLoopFuture<()> {
            let tick = currentTimeNanos()
            return el.flatSubmit {
                if (!fd.isAvailable(at: tick)) {
                    return signalFailure()
                } else {
                    let probeResponse = self.messagingClient
                            .sendMessageBestEffort(recipient: subject, msg: self.probeRequest)
                            .hop(to: self.el) // make sure we always process these from the same event loop

                    probeResponse.whenSuccess { response in
                        switch(response.content) {
                            case .probeResponse:
                                // TODO handle probe status / fail after too many probes in initializing state
                                fd.heartbeat()
                                return
                            default:
                                return
                        }
                    }

                    return self.el.makeSucceededFuture(())
                }
            }
        }

        return run
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
