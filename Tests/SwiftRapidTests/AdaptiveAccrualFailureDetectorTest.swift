import XCTest
import Concurrency
@testable import SwiftRapid

class AdaptiveAccrualFailureDetectorTest: XCTestCase {

    func testAtomicRefCAS() throws {
        struct Foo {
            let a: [Int]
        }

        let foo1 = Foo(a: [1])
        let foo2 = Foo(a: [2])

        let ref = AtomicReference<Foo>(initialValue: foo1)

        let old = ref.value
        let update = ref.compareAndSet(expect: old, newValue: foo2)
        XCTAssertTrue(update)

        var array = [Int]()

        // when we use the contents of the array in old, this test fails
        // when we don't, it works
        // so somehow, reading the contents of the array inside of the struct mutates the pointer of the struct??
        array.append(contentsOf: old.a)

        let immutableArray = array

        let old2 = ref.value
        let update2 = ref.compareAndSet(expect: old2, newValue: Foo(a: immutableArray))
        XCTAssertTrue(update2)
    }

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

    func testFailedAfterIfMissedHeartbeats() throws {
        let fd = createFailureDetector(clock: generateTime(intervals: [0, 100, 100, 100, 1000, 10000]))
        fd.heartbeat() // 100
        fd.heartbeat() // 200
        fd.heartbeat() // 300

        XCTAssertTrue(fd.isAvailable()) // 1300
        XCTAssertFalse(fd.isAvailable()) // 11300
    }

    private func generateTime(intervals: [UInt64]) -> () -> UInt64 {
        guard let head = intervals.first else {
            return {
                0
            }
        }
        let tail = intervals.dropFirst()
        var times = tail.reduce([head], { (acc, i) in acc + [acc.last! + i] })

        print(times)

        return {
            print("Tick")
            let time = times.first!
            times.removeFirst()
            return time
        }
    }

    private func createFailureDetector(threshold: Double = 0.2, maxSampleSize: Int = 1000, scalingFactor: Double = 0.9, clock: @escaping () -> UInt64) -> AdaptiveAccrualFailureDetector {
        return try! AdaptiveAccrualFailureDetector(threshold: threshold, maxSampleSize: maxSampleSize, scalingFactor: scalingFactor, clock: clock)
    }

}
