import NIO
import NIOConcurrencyHelpers
import XCTest
import Dispatch
@testable import SwiftRapid

class ClusterTests: XCTestCase {

    let instances = ConcurrentTestDictionary<Endpoint, RapidCluster>()

    let basePort = 1234
    let portCounter = NIOAtomic.makeAtomic(value: 1235)

    var settings = Settings()

    override func setUp() {
        settings = Settings()
        portCounter.store(1235)
        instances.clear()
    }

    override func tearDown() {
        for instance in instances.values() {
            try! instance.shutdown()
        }
    }


    func testSingleNodeJoinsThroughSeed() throws {
        let seedEndpoint = addressFromParts("127.0.0.1", basePort)
        try createCluster(numNodes: 1, seedEndpoint: seedEndpoint)
        verifyCluster(expectedSize: 1)
        try extendCluster(numNodes: 1, seed: seedEndpoint)
        verifyCluster(expectedSize: 2)
    }

    func createCluster(numNodes: Int, seedEndpoint: Endpoint) throws {
        let seedNode: RapidCluster = try buildCluster(endpoint: seedEndpoint).start()
        instances.put(key: seedEndpoint, value: seedNode)
        XCTAssertEqual(1, try seedNode.getMemberList().count)
        if (numNodes >= 2) {
            try extendCluster(numNodes: numNodes - 1, seed: seedEndpoint)
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

    func extendCluster(numNodes: Int, seed: Endpoint) throws {
        // TODO make async - need a countdownlatch
        let joiningEndpoint = addressFromParts("127.0.0.1", portCounter.add(1))
        let joiningNode = try buildCluster(endpoint: joiningEndpoint).join(seedEndpoint: seed)
        instances.put(key: joiningEndpoint, value: joiningNode)
    }

    func buildCluster(endpoint: Endpoint) -> RapidCluster.Builder {
        RapidCluster.Builder.with {
            $0.host = String(decoding: endpoint.hostname, as: UTF8.self)
            $0.port = Int(endpoint.port)
        }
    }


    static var allTests = [
        ("testSingleNodeJoinsThroughSeed", testSingleNodeJoinsThroughSeed)
    ]

}






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

    func values() -> [V] {
        queue.sync() {
            return Array(self.dictionary.values)
        }
    }

    func clear() {
        queue.async(flags: .barrier) {
            self.dictionary = [:]
        }
    }

}