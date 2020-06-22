import XCTest
import libkern
@testable import SwiftRapid

class AdaptiveAccrualFailureDetectorTest: XCTestCase {

    func testAvailableBeforeFirstHeartbeat() throws {
        let fd = createFailureDetector(clock: generateTime(intervals: [0, 100, 100, 100]))
        XCTAssertTrue(fd.isAvailable())
    }

    func testAvailableAfterAFewHeartbeats() throws {
        let fd = createFailureDetector(clock: generateTime(intervals: [0, 100, 100, 100]))
        fd.heartbeat()
        fd.heartbeat()
        fd.heartbeat()
        XCTAssertTrue(fd.isAvailable())
    }

    func testFailedAfterMissedHeartbeats() throws {
        let fd = createFailureDetector(clock: generateTime(intervals: [0, 100, 100, 100, 1000, 10000]))
        fd.heartbeat() // 100
        fd.heartbeat() // 200
        fd.heartbeat() // 300

        XCTAssertTrue(fd.isAvailable()) // 1300
        XCTAssertFalse(fd.isAvailable()) // 11300
    }

    func testAvailableAfterFailures() throws {
        let regularIntervals: [UInt64] = Array(repeating: 1000, count: 1000)
        let intervalsWithPauses = [5 * 60 * 1000, 100, 900, 100, 7000, 100, 900, 100, 900]
        let intervals = regularIntervals + intervalsWithPauses.map { UInt64($0) }
        let fd = createFailureDetector(clock: generateTime(intervals: intervals))
        for _ in 0..<1000 {
            fd.heartbeat()
        }
        XCTAssertFalse(fd.isAvailable()) // after the 5 minutes pause
        fd.heartbeat()
        XCTAssertTrue(fd.isAvailable()) // after the recovery
        fd.heartbeat()
        XCTAssertFalse(fd.isAvailable()) // after 7 seconds pause
        fd.heartbeat()
        XCTAssertTrue(fd.isAvailable()) // after the recovery
    }

    func testAvailableWhenLatencyFluctuates() throws {
        let regularIntervals: [UInt64] = Array(repeating: 1000, count: 1000)
        let slowerIntervals: [UInt64] = [1100, 1100, 1150, 1200, 1200, 1000, 1100]
        let fd = createFailureDetector(scalingFactor: 0.8, clock: generateTime(intervals: regularIntervals + slowerIntervals))
        for _ in 0..<1000 {
            fd.heartbeat()
        }
        XCTAssertTrue(fd.isAvailable())
        fd.heartbeat()
        XCTAssertTrue(fd.isAvailable())
        fd.heartbeat()
        XCTAssertTrue(fd.isAvailable())
        fd.heartbeat()
        XCTAssertTrue(fd.isAvailable())
    }

    func testUseOfMaxSampleSize() throws {
        let intervals: [UInt64] = [0, 100, 100, 100, 1000, 900, 900, 900, 900, 900]
        let fd = createFailureDetector(maxSampleSize: 3, clock: generateTime(intervals: intervals))

        // 100 ms
        fd.heartbeat()
        fd.heartbeat()
        fd.heartbeat()
        let suspicion1 = fd.suspicion()

        // 1000 ms
        fd.heartbeat()

        // 900 ms
        fd.heartbeat()
        fd.heartbeat()
        fd.heartbeat()

        // by now only 900ms samples should be left
        let suspicion2 = fd.suspicion()

        XCTAssertEqual(suspicion1, suspicion2)
    }

    private func generateTime(intervals: [UInt64]) -> () -> UInt64 {
        guard let head = intervals.first else {
            return {
                0
            }
        }
        let tail = intervals.dropFirst()
        var times = tail.reduce([head], { (acc, i) in acc + [acc.last! + i] })

        return {
            let time = times.first!
            times.removeFirst()
            return time
        }
    }

    private func createFailureDetector(threshold: Double = 0.2, maxSampleSize: Int = 1000, scalingFactor: Double = 0.9, clock: @escaping () -> UInt64) -> AdaptiveAccrualFailureDetector {
        return try! AdaptiveAccrualFailureDetector(threshold: threshold, maxSampleSize: maxSampleSize, scalingFactor: scalingFactor, clock: clock)
    }

}
