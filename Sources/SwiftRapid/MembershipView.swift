
import Foundation
import xxHash_Swift

/// This node's view of the membership
///
/// Observer -> Subject relationships are arranged on an expander graph modeled via K rings of ordered sets
///
/// Since there's no implementation of an always-sorted / navigable set in Swift we work around this by
/// using an array/set combo and re-sort the collection explicitly after all nodes have been added / removed.
/// The graph is only altered during a view change so this is a safe thing to do.
///
/// This class is not thread safe
///
final class MembershipView {

    private let K: Int
    private var rings = [SortableSet<Endpoint>]()
    private var seenIdentifiers: Set<NodeId>
    private var currentConfiguration: Configuration

    private var allNodes: Set<Endpoint>
    private var cachedObservers: [Endpoint : [Endpoint]] = [:]

    private var shouldUpdateConfigurationId = true

    init(K: Int) {
        assert(K > 0)
        self.K = K
        self.allNodes = Set()
        self.seenIdentifiers = Set()
        self.currentConfiguration = Configuration(nodeIds: seenIdentifiers, endpoints: [])
        let range = 0..<K
        for k in range {
            rings.append(SortableSet<Endpoint>(seed: k))
        }
    }

    convenience init(K: Int, nodeIds: [NodeId], endpoints: [Endpoint]) {
        self.init(K: K)
        self.allNodes = Set(endpoints)
        self.seenIdentifiers = Set(nodeIds)
        self.rings = stride(from: 0, to: K, by: 1).map { k in SortableSet<Endpoint>(endpoints, seed: k) }
        for n in 0..<K {
            self.rings[n].sort()
        }
    }

    func isSafeToJoin(node: Endpoint, uuid: NodeId) -> JoinStatusCode {
        switch (allNodes.contains(node), seenIdentifiers.contains(uuid)) {
            case (true, true):
                return JoinStatusCode.sameNodeAlreadyInRing
            case (false, true):
                return JoinStatusCode.uuidAlreadyInRing
            case (true, false):
                return JoinStatusCode.hostnameAlreadyInRing
            case (false, false):
                return JoinStatusCode.safeToJoin
        }
    }

    func isHostPresent(_ node: Endpoint) -> Bool {
        return allNodes.contains(node)
    }

    func isIdentifierPresent(_ uuid: NodeId) -> Bool {
        return seenIdentifiers.contains(uuid)
    }

    func getObserversOf(_ node: Endpoint) throws -> [Endpoint] {
        if (!allNodes.contains(node)) {
            throw MembershipViewError.NodeNotInRingError(node)
        }
        guard let observers = cachedObservers[node] else {
            let computed = computeObserversOf(node)
            cachedObservers[node] = computed
            return computed
        }
        return observers
    }

    func getExpectedObserversOf(node: Endpoint) -> [Endpoint] {
        if (rings[0].isEmpty) {
            return []
        }
        return getPredecessorsOf(node)
    }

    func getSubjectsOf(node: Endpoint) throws -> [Endpoint] {
        if (!allNodes.contains(node)) {
            throw MembershipViewError.NodeNotInRingError(node)
        }

        if(rings[0].count <= 1) {
            return []
        }
        return getPredecessorsOf(node)
    }

    func getCurrentConfigurationId() -> UInt64 {
        if (shouldUpdateConfigurationId) {
            updateCurrentConfiguration()
            shouldUpdateConfigurationId = false
        }
        return currentConfiguration.configurationId
    }

    func getCurrentConfiguration() -> Configuration {
        if (shouldUpdateConfigurationId) {
            updateCurrentConfiguration()
            shouldUpdateConfigurationId = false
        }
        return currentConfiguration
    }

    func getRing(k: Int) -> SortableSet<Endpoint> {
        return rings[k]
    }

    func getRingNumbers(observer: Endpoint, subject: Endpoint) -> [Int] {
        return []
    }

    func getMembershipSize() -> Int {
        return allNodes.count
    }

    private func getPredecessorsOf(_ node: Endpoint) -> [Endpoint] {
        return rings.compactMap { ring in
            ring.lower(node) != nil ? ring.lower(node) : ring.last
        }
    }

    private func computeObserversOf(_ node: Endpoint) -> [Endpoint] {
        if (rings[0].count <= 1) {
            return []
        }

        return rings.compactMap { ring in
            guard let successor = ring.higher(node) else {
                return ring.first()
            }
            return successor
        }
    }

    func ringAdd(node: Endpoint, nodeId: NodeId) throws {
        if (isIdentifierPresent(nodeId)) {
            throw MembershipViewError.UUIDAlreadySeenError(node, nodeId)
        }
        if (rings[0].contains(node)) {
            throw MembershipViewError.NodeAlreadyInRingError(node)
        }
        var affectedSubjects = Set<Endpoint>()
        for k in 0..<K {
            rings[k].append(node)

            // TODO this is, of course, a performance nightmare
            // TODO offer variant to add / remove many nodes at once (collecting errors) and sort after the fact
            // TODO or implement / find an implementation of an always-sorted set with custom sort
            rings[k].sort()

            if let subject = rings[k].lower(node) {
                affectedSubjects.insert(subject)
            }
        }
        allNodes.insert(node)
        for subject in affectedSubjects {
            cachedObservers.removeValue(forKey: subject)
        }
        seenIdentifiers.insert(nodeId)
        shouldUpdateConfigurationId = true
    }

    func ringDelete(node: Endpoint) throws {
        if (!rings[0].contains(node)) {
            throw MembershipViewError.NodeNotInRingError(node)
        }
        var affectedSubjects = Set<Endpoint>()
        for k in 0..<K {
            if let oldSubject = rings[k].lower(node) {
                affectedSubjects.insert(oldSubject)
            }
            rings[k].remove(node)
            cachedObservers.removeValue(forKey: node)
        }
        allNodes.remove(node)

        for subject in affectedSubjects {
            cachedObservers.removeValue(forKey: subject)
        }

        shouldUpdateConfigurationId = true
    }

    private func updateCurrentConfiguration() {
        currentConfiguration = Configuration(nodeIds: seenIdentifiers, endpoints: rings[0].contents)
    }

}

struct Configuration {
    private let nodeIds: Set<NodeId>
    private let endpoints: [Endpoint]
    let configurationId: UInt64

    init(nodeIds: Set<NodeId>, endpoints: [Endpoint]) {
        self.nodeIds = nodeIds
        self.endpoints = endpoints

        // compute stable hash across all nodes
        var hash: UInt64 = 1
        for nodeId in nodeIds {
            hash = hash &+ XXH64.digest(byteArray(from: nodeId.high))
            hash = hash &+ numericCast(XXH64.digest(byteArray(from: nodeId.low)))
        }
        for endpoint in endpoints {
            hash = hash &+ numericCast(XXH64.digest(endpoint.hostname))
            hash = hash &+ numericCast(XXH64.digest(byteArray(from: endpoint.port)))
        }
        self.configurationId = hash
    }

}



enum MembershipViewError: Error, Equatable {
    case NodeNotInRingError(Endpoint)
    case NodeAlreadyInRingError(Endpoint)
    case UUIDAlreadySeenError(Endpoint, NodeId)
}

extension Endpoint: RingHashable {
    public func ringHash(seed: Int) -> UInt64 {
        // TODO find another implementation of the XX hash that accepts data directly as bytes
        return XXH64.digest(self.hostname.base64EncodedString() + "\(self.port)", seed: UInt64(seed))
    }
}