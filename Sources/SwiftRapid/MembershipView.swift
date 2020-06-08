
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

    /// Initializes an empty membership view
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

    /// Initializes a membership view based on a given set of nodes and identifiers
    convenience init(K: Int, nodeIds: [NodeId], endpoints: [Endpoint]) {
        self.init(K: K)
        self.allNodes = Set(endpoints)
        self.seenIdentifiers = Set(nodeIds)
        self.rings = stride(from: 0, to: K, by: 1).map { k in SortableSet<Endpoint>(endpoints, seed: k) }
        for n in 0..<K {
            self.rings[n].sort()
        }
    }

    /// Query if a new node can join the membership group or if there is a problem
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

    /// Query if the host is already part of the membership group
    func isHostPresent(_ node: Endpoint) -> Bool {
        return allNodes.contains(node)
    }

    /// Query if the unique identifier is known to this membership view
    func isIdentifierPresent(_ uuid: NodeId) -> Bool {
        return seenIdentifiers.contains(uuid)
    }

    /// Retrieves all the monitoring nodes (observers) of a given subject node
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

    /// Retrieves all the monitoring nodes (observers) of a given subject node
    /// prior to it being added to the membership group
    func getExpectedObserversOf(node: Endpoint) -> [Endpoint] {
        if (rings[0].isEmpty) {
            return []
        }
        return getPredecessorsOf(node)
    }

    /// Retrieves all the monitored nodes (subjects) of a given observer node
    func getSubjectsOf(node: Endpoint) throws -> [Endpoint] {
        if (!allNodes.contains(node)) {
            throw MembershipViewError.NodeNotInRingError(node)
        }

        if(rings[0].count <= 1) {
            return []
        }
        return getPredecessorsOf(node)
    }

    /// Query the identifier of the current configuration
    func getCurrentConfigurationId() -> UInt64 {
        if (shouldUpdateConfigurationId) {
            updateCurrentConfiguration()
            shouldUpdateConfigurationId = false
        }
        return currentConfiguration.configurationId
    }

    /// Retrieves the current Configuration
    func getCurrentConfiguration() -> Configuration {
        if (shouldUpdateConfigurationId) {
            updateCurrentConfiguration()
            shouldUpdateConfigurationId = false
        }
        return currentConfiguration
    }

    /// Retrieves the nodes in the order of the k'th ring
    func getRing(k: Int) -> SortableSet<Endpoint> {
        return rings[k]
    }

    /// Retrieves the ring numbers of an observer for the given subject such that
    /// subject is a successor of observer on ring[k]
    func getRingNumbers(observer: Endpoint, subject: Endpoint) throws -> [Int] {
        let subjects = try getSubjectsOf(node: observer)
        if (subjects.isEmpty) {
            return []
        }
        var ringIndexes = [Int]()
        var ringNumber = 0
        for node in subjects {
            if (node == subject) {
                ringIndexes.append(ringNumber)
            }
            ringNumber += 1
        }
        return ringIndexes
    }

    /// Queries the amount of members of the membership group
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

    /// Adds a node to the view
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

    /// Removes a node from the view
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
        let seed = UInt64(seed)
        return XXH64.digest(self.hostname, seed: seed)
                &+ XXH64.digest(byteArray(from: self.port), seed: seed)
    }
}