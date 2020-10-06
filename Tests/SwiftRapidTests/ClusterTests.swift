import NIO
import NIOConcurrencyHelpers
import XCTest
import Dispatch
import Foundation
@testable import SwiftRapid
import Backtrace
import Logging

/// Tests common and edge cluster creation / change / failure scenarios
///
/// /!\ Make sure your shell has a high enough open file limit! 1024 won't do at the level of concurrency of this test.
///     You can change it using e.g. `ulimit -n 20000` on linux
///
class ClusterTests: XCTestCase {

    let instances = ConcurrentTestDictionary<Endpoint, RapidCluster>()

    var group = MultiThreadedEventLoopGroup(numberOfThreads: 4)

    let basePort = 1234
    let portCounter = NIOAtomic.makeAtomic(value: 1235)

    var settings = Settings()

    var addMetadata = true
    var useStaticFD = false
    var staticProvider: StaticFailureDetectorProvider? = nil

    override class func setUp() {
        super.setUp()
        LoggingSystem.bootstrap { label in
            var logHandler = StreamLogHandler.standardOutput(label: label)
            logHandler.logLevel = .notice
            return logHandler
        }
    }

    override func setUp() {
        useStaticFD = false
        Backtrace.install()
        settings = Settings()
        portCounter.store(1235)
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        instances.clear()
    }

    override func tearDown() {
        for instance in instances.values() {
            try! instance.shutdown()
        }
        staticProvider = nil
        try! group.syncShutdownGracefully()
    }

    func testSingleNodeJoinsThroughSeed() throws {
        let seedEndpoint = addressFromParts("127.0.0.1", basePort)
        try createCluster(numNodes: 1, seedEndpoint: seedEndpoint)
        verifyCluster(expectedSize: 1)
        try extendCluster(numNodes: 1, seed: seedEndpoint)
        verifyCluster(expectedSize: 2)
    }

    func testTenNodesJoinSequentially() throws {
        let numNodes = 10
        let seedEndpoint = addressFromParts("127.0.0.1", basePort)
        try createCluster(numNodes: 1, seedEndpoint: seedEndpoint)
        verifyCluster(expectedSize: 1)
        for i in 0..<numNodes {
            try extendCluster(numNodes: 1, seed: seedEndpoint)
            waitAndVerifyAgreement(expectedSize: i + 2, maxTries: 5, interval: 1.0)
        }
    }

    func testTwentyNodesJoinSequentially() throws {
        let numNodes = 20
        let seedEndpoint = addressFromParts("127.0.0.1", basePort)
        try createCluster(numNodes: 1, seedEndpoint: seedEndpoint)
        verifyCluster(expectedSize: 1)
        for i in 0..<numNodes {
            try extendCluster(numNodes: 1, seed: seedEndpoint)
            waitAndVerifyAgreement(expectedSize: i + 2, maxTries: 5, interval: 1.0)
        }
    }

    func testFiftyNodesJoinInParallel() throws {
        addMetadata = false
        let numNodes = 50
        let seedEndpoint = addressFromParts("127.0.0.1", basePort)
        try createCluster(numNodes: numNodes, seedEndpoint: seedEndpoint)
        waitAndVerifyAgreement(expectedSize: numNodes, maxTries: 5, interval: 1.0)
        verifyCluster(expectedSize: numNodes)
        verifyMetadata(expectedSize: numNodes)
    }

    func testFiftyNodesJoinTwentyNodeCluster() throws {
        let numNodesPhase1 = 20
        let numNodesPhase2 = 50
        let seedEndpoint = addressFromParts("127.0.0.1", basePort)
        try createCluster(numNodes: numNodesPhase1, seedEndpoint: seedEndpoint)
        waitAndVerifyAgreement(expectedSize: numNodesPhase1, maxTries: 5, interval: 1.0)
        verifyMetadata(expectedSize: numNodesPhase1)
        try extendCluster(numNodes: numNodesPhase2, seed: seedEndpoint)
        waitAndVerifyAgreement(expectedSize: numNodesPhase1 + numNodesPhase2, maxTries: 5, interval: 1.0)
        verifyMetadata(expectedSize: numNodesPhase1 + numNodesPhase2)
    }

    func testOneNodeFails() throws {
        useFastFailureDetectorTimeouts()
        let numNodes = 6
        let seedEndpoint = addressFromParts("127.0.0.1", basePort)
        try createCluster(numNodes: numNodes, seedEndpoint: seedEndpoint)
        waitAndVerifyAgreement(expectedSize: numNodes, maxTries: 5, interval: 1.0)
        // we need to wait for the FDs to have gathered at least one interval value
        sleep(UInt32(settings.failureDetectorInterval.nanoseconds * 3 / 1000000000))
        let victim = addressFromParts("127.0.0.1", basePort + 3)
        failSomeNodes(nodesToFail: [victim])
        waitAndVerifyAgreement(expectedSize: numNodes - 1, maxTries: 10, interval: 2.0)
    }

    func testThreeNodeFail() throws {
        useFastFailureDetectorTimeouts()
        let numNodes = 15
        let numFailing = 3
        let seedEndpoint = addressFromParts("127.0.0.1", basePort)
        try createCluster(numNodes: numNodes, seedEndpoint: seedEndpoint)
        // we need to wait for the FDs to have gathered at least one interval value
        waitAndVerifyAgreement(expectedSize: numNodes, maxTries: 5, interval: 1.0)
        sleep(UInt32(settings.failureDetectorInterval.nanoseconds * 3 / 1000000000))
        var failingNodes = [Endpoint]()
        for failingPort in (basePort + 2)..<(basePort + 2 + numFailing) {
            failingNodes.append(addressFromParts("127.0.0.1", failingPort))
        }
        failSomeNodes(nodesToFail: failingNodes)
        waitAndVerifyAgreement(expectedSize: numNodes - numFailing, maxTries: 10, interval: 2.0)
    }

    func testConcurrentNodesJoinAndFail() throws {
        useFastFailureDetectorTimeouts()
        let numNodes = 17
        let numFailing = 3
        let numNodesPhase2 = 5
        let seedEndpoint = addressFromParts("127.0.0.1", basePort)
        try createCluster(numNodes: numNodes, seedEndpoint: seedEndpoint)
        waitAndVerifyAgreement(expectedSize: numNodes, maxTries: 5, interval: 1.0)
        // we need to wait for the FDs to have gathered at least one interval value
        sleep(UInt32(settings.failureDetectorInterval.nanoseconds * 5 / 1000000000))
        var failingNodes = [Endpoint]()
        for failingPort in (basePort + 2)..<(basePort + 2 + numFailing) {
            failingNodes.append(addressFromParts("127.0.0.1", failingPort))
        }
        failSomeNodes(nodesToFail: failingNodes)
        sleep(2)
        try! extendCluster(numNodes: numNodesPhase2, seed: seedEndpoint)
        waitAndVerifyAgreement(expectedSize: numNodes - numFailing + numNodesPhase2, maxTries: 20, interval: 2.0)
    }

    func failRandomQuarterOfNodes() throws {
        settings.failureDetectorInterval = TimeAmount.milliseconds(500)
        settings.messagingClientProbeRequestTimeout = TimeAmount.milliseconds(200)
        useStaticFD = true
        let numNodes = 40
        let numFailingNodes = 10
        let seedEndpoint = addressFromParts("127.0.0.1", basePort)
        try createCluster(numNodes: numNodes, seedEndpoint: seedEndpoint)
        verifyCluster(expectedSize: numNodes)
        print("Cluster formed")
        var failingNodes = [Endpoint]()
        while failingNodes.count != numFailingNodes {
            let node = instances.keys().randomElement()!
            if (!failingNodes.contains(node)) {
                failingNodes.append(node)
                staticProvider?.addFailedNode(node: node)
            }
        }
        for node in failingNodes {
            try! instances.get(key: node)?.shutdown()
            print("Down")
            instances.remove(key: node)
        }
        waitAndVerifyAgreement(expectedSize: numNodes - failingNodes.count, maxTries: 20, interval: 1000)
        XCTAssertEqual(numNodes - failingNodes.count, instances.values().count)

    }



    // ~~~ utility methods

    func useFastFailureDetectorTimeouts() {
        settings.failureDetectorInterval = TimeAmount.milliseconds(1200)
        settings.messagingClientProbeRequestTimeout = TimeAmount.milliseconds(1200)
    }

    func createCluster(numNodes: Int, seedEndpoint: Endpoint) throws {
        let seedNode: RapidCluster = try buildCluster(endpoint: seedEndpoint).start()
        instances.put(key: seedEndpoint, value: seedNode)
        XCTAssertEqual(1, try seedNode.getMemberList().count)
        if (numNodes >= 2) {
            try extendCluster(numNodes: numNodes - 1, seed: seedEndpoint)
        }
    }

    func extendCluster(numNodes: Int, seed: Endpoint) throws {
        let dispatchQueue: DispatchQueue = DispatchQueue(label: String(describing: "test.cluster.extender"), attributes: .concurrent)
        let counter = NIOAtomic.makeAtomic(value: numNodes)
        for _ in 0..<numNodes {
            dispatchQueue.async {
                let joiningEndpoint = addressFromParts("127.0.0.1", self.portCounter.add(1))
                do {
                    let joiningNode = try self
                            .buildCluster(endpoint: joiningEndpoint)
                            .join(seedEndpoint: seed)
                    self.instances.put(key: joiningEndpoint, value: joiningNode)
                    counter.sub(1)
                } catch {
                    print("Unexpected error during cluster extension for \(self.portCounter.load()): \(error)")
                }
            }
        }
        while(counter.load() > 0) {
            Thread.sleep(forTimeInterval: 1.0)
        }
    }

    func failSomeNodes(nodesToFail: [Endpoint]) {
        let dispatchQueue: DispatchQueue = DispatchQueue(label: String(describing: "test.cluster.killer"), attributes: .concurrent)
        let counter = NIOAtomic.makeAtomic(value: nodesToFail.count)
        for _ in 0..<nodesToFail.count {
            dispatchQueue.async {
                for nodeToFail in nodesToFail {
                    if (self.instances.get(key: nodeToFail) != nil) {
                        do {
                            try self.instances.get(key: nodeToFail)?.shutdown()
                        } catch {
                            print(error)
                        }
                        let _ = self.instances.remove(key: nodeToFail)
                        counter.sub(1)
                    } else {
                        print("******* Cluster instance for \(nodeToFail.port) not there???")

                    }
                }
            }
        }
        while(counter.load() > 0) {
            Thread.sleep(forTimeInterval: 1.0)
        }
    }

    func buildCluster(endpoint: Endpoint) -> RapidCluster.Builder {
        RapidCluster.Builder.with {
            $0.host = String(decoding: endpoint.hostname, as: UTF8.self)
            $0.port = Int(endpoint.port)
            $0.settings = settings
            if(useStaticFD) {
                staticProvider = StaticFailureDetectorProvider(el: group.next())
                $0.edgeFailureDetectorProvider = staticProvider
            }
            if (addMetadata) {
                $0.metadata = Metadata.with {
                    $0.metadata = ["Key": endpoint.textFormatString().data(using: .utf8)!]
                }
            }
        }
    }


    func verifyCluster(expectedSize: Int) {
        do {
            let memberList = try instances.values().first?.getMemberList()
            for node in instances.values() {
                XCTAssertEqual(expectedSize, try node.getMemberList().count)
                XCTAssertEqual(memberList, try node.getMemberList())
                // TODO verify metadata
            }
        } catch {
            XCTFail()
        }
    }

    func verifyMetadata(expectedSize: Int) {
        for instance in instances.values() {
            XCTAssertEqual(try! instance.getClusterMetadata().count, expectedSize, String(instance.getEndpoint().port))
        }
    }

    func waitAndVerifyAgreement(expectedSize: Int, maxTries: Int, interval: TimeInterval) {
        var tries = maxTries
        do {
            while (tries >= 0) {
                tries = tries - 1
                var ready = true
                let memberList: [Endpoint] = try instances.values().first?.getMemberList() ?? []
                for node in instances.values() {
                    if (!(try node.getMemberList().count == expectedSize && node.getMemberList() == memberList)) {
                        ready = false
                    }
                }
                if (!ready) {
                    Thread.sleep(forTimeInterval: interval)
                } else {
                    break
                }
            }
        } catch {
            print("*** ERROR while waiting for agreement: \(error)")
        }
        verifyCluster(expectedSize: expectedSize)
    }


    static var allTests = [
        ("testSingleNodeJoinsThroughSeed", testSingleNodeJoinsThroughSeed),
        ("testTenNodesJoinSequentially", testTenNodesJoinSequentially),
        ("testTwentyNodesJoinSequentially", testTwentyNodesJoinSequentially),
        ("testFiftyNodesJoinInParallel", testFiftyNodesJoinInParallel),
        ("testFiftyNodesJoinTwentyNodeCluster", testFiftyNodesJoinTwentyNodeCluster),
        ("testOneNodeFails", testOneNodeFails),
        ("testThreeNodeFail", testThreeNodeFail),
        ("testConcurrentNodesJoinAndFail", testConcurrentNodesJoinAndFail),
        ("failRandomQuarterOfNodes", failRandomQuarterOfNodes)
    ]

}



/// TODO check if this is an okay approach
class ConcurrentTestDictionary<K, V> where K: Hashable {
    private let queue = DispatchQueue(label: "concurrent.dict", attributes: .concurrent)
    private var dictionary = Dictionary<K, V>()

    func put(key: K, value: V) {
        queue.async(flags: .barrier) {
            self.dictionary[key] = value
        }
    }

    func get(key: K) -> V? {
        queue.sync() {
            return self.dictionary[key]
        }
    }

    func remove(key: K) -> () {
        queue.async(flags: .barrier) {
            self.dictionary.removeValue(forKey: key)
        }
    }

    func values() -> [V] {
        queue.sync() {
            return Array(self.dictionary.values)
        }
    }

    func keys() -> [K] {
        queue.sync() {
            Array(self.dictionary.keys)
        }
    }

    func clear() {
        queue.async(flags: .barrier) {
            self.dictionary = [:]
        }
    }

}

class StaticFailureDetectorProvider: EdgeFailureDetectorProvider {

    private let el: EventLoop
    private var failedNodes = [Endpoint]()

    init(el: EventLoop) {
        self.el = el
    }

    func createInstance(subject: Endpoint, signalFailure: @escaping (Endpoint) -> ()) throws -> () -> EventLoopFuture<()> {
        {
            if (self.failedNodes.contains(subject)) {
                signalFailure(subject)
            }
            return self.el.makeSucceededFuture(())
        }
    }

    func addFailedNode(node: Endpoint) {
        failedNodes.append(node)
    }
}

