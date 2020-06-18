import Foundation
import NIO
import Concurrency

///
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
class AdaptiveAccrualFailureDetectorProvider: EdgeFailureDetectorProvider {
    func createInstance(subject endpoint: Endpoint) -> () -> EventLoopFuture<FailureDetectionResult> {
        fatalError("createInstance(subject:) has not been implemented")
    }
}

class AdaptiveAccrualFailureDetector {

    private let threshold: Double
    private let maxSampleSize: Int
    private let scalingFactor: Double

    private let clock: () -> UInt64

    private let state = AtomicReference<State>(initialValue: State(intervals: [], freshnessPoint: nil))

    init(threshold: Double, maxSampleSize: Int, scalingFactor: Double, clock: @escaping () -> UInt64) throws {
        self.threshold = threshold
        self.maxSampleSize = maxSampleSize
        self.scalingFactor = scalingFactor
        self.clock = clock

        try require(threshold > 0.0, "Threshold must be strictly positive")
        try require(maxSampleSize > 0, "Max sample size must be strictly positive")
        try require(scalingFactor > 0.0, "Scaling factor must be strictly positive")
    }

    func isAvailable(timestamp: UInt64) -> Bool {
        return suspicion(timestamp: timestamp) < threshold
    }

    func heartbeat() {
        let timestamp = clock()
        let oldState: State = state.value
        var newIntervals = [UInt64](oldState.intervals)

        if let freshnessPoint = oldState.freshnessPoint {
            let tΔ = timestamp - freshnessPoint
            newIntervals.append(tΔ)
        } else {
            // this is heartbeat from a new resource
            // according to the algorithm do not add any initial history
        }

        let newState = State(intervals: newIntervals, freshnessPoint: timestamp)

        // if we won the race then update else try again
        if (!state.compareAndSet(expect: oldState, newValue: newState)) {
            return heartbeat()
        }
    }

    private func suspicion(timestamp: UInt64) -> Double {
        let oldState: State = state.value

        guard let freshnessPoint = oldState.freshnessPoint else {
            // treat unmanaged connections, e.g. without initial state, as healthy connections
            return 0.0
        }

        if (oldState.intervals.isEmpty) {
            // treat unmanaged connections, e.g. with zero heartbeats, as healthy connections
            return 0.0
        } else {
            let tΔ = timestamp - freshnessPoint
            let S = oldState.intervals
            let SLength = S.count
            let StΔLength = S.filter { interval in Double(interval) <= Double(tΔ) * scalingFactor }.count

            return Double(StΔLength) / Double(SLength)
        }
    }

    struct State {
        var intervals: [UInt64]
        var freshnessPoint: Optional<UInt64>
    }

}
