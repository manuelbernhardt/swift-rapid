import Foundation

/// An ordered set that can be resorted and provides utility methods for navigating the elements
///
/// TODO consider renaming to reflect the specific "ring" use case esp. in regards to sorting different sets with specific (stable) seeds
///
/// Based on https://github.com/apple/swift-package-manager/blob/master/swift-tools-support-core/Sources/TSCBasic/OrderedSet.swift
///
///

public struct SortableSet<E: Hashable & RingHashable>: Equatable, Collection {
    public typealias Element = E
    public typealias Index = Int

    #if swift(>=4.1.50)
    public typealias Indices = Range<Int>
    #else
public typealias Indices = CountableRange<Int>
    #endif

    private let ringHashSeed: Int
    private var array: [Element]
    private var set: Set<Element>

    /// Creates an empty sorted set.
    public init() {
        self.ringHashSeed = 0
        self.array = []
        self.set = Set()
    }

    /// Created an empty sorted set with a hash seed
    public init(seed: Int) {
        self.ringHashSeed = seed
        self.array = []
        self.set = Set()
    }

    /// Creates a sorted set with the contents of `array`.
    ///
    /// If an element occurs more than once in `element`, only the first one
    /// will be included.
    public init(_ array: [Element]) {
        self.init()
        for element in array {
            append(element)
        }
    }

    /// Creates a sorted set with the contents of `array`.
    ///
    /// If an element occurs more than once in `element`, only the first one
    /// will be included.
    public init(_ array: [Element], seed: Int) {
        self.init(seed: seed)
        for element in array {
            append(element)
        }
    }

    // MARK: Working with an ordered set

    /// The number of elements the ordered set stores.
    public var count: Int { return array.count }

    /// Returns `true` if the set is empty.
    public var isEmpty: Bool { return array.isEmpty }

    /// Returns the contents of the set as an array.
    public var contents: [Element] { return array }

    /// Returns `true` if the ordered set contains `member`.
    public func contains(_ member: Element) -> Bool {
        return set.contains(member)
    }

    /// Adds an element to the ordered set.
    ///
    /// If it already contains the element, then the set is unchanged.
    ///
    /// - returns: True if the item was inserted.
    @discardableResult
    public mutating func append(_ newElement: Element) -> Bool {
        let inserted = set.insert(newElement).inserted
        if inserted {
            array.append(newElement)
        }
        return inserted
    }

    /// Retrieves the first element of the set
    public func first() -> Element? {
        return array.first
    }

    /// Retrieves the element after the given one
    public func higher(_ node: Element) -> Element? {
        return array.firstIndex(of: node).flatMap { index in
            array.indices.contains(index + 1) ? array[index + 1] : nil
        }
    }

    /// Retrieves the element before the given one
    public func lower(_ node: Element) -> Element? {
        return array.lastIndex(of: node).flatMap { index in
            array.indices.contains(index - 1) ? array[index - 1] : nil
        }
    }

    /// Remove and return the element at the beginning of the ordered set.
    public mutating func removeFirst() -> Element {
        let firstElement = array.removeFirst()
        set.remove(firstElement)
        return firstElement
    }

    /// Remove and return the element at the end of the ordered set.
    public mutating func removeLast() -> Element {
        let lastElement = array.removeLast()
        set.remove(lastElement)
        return lastElement
    }

    /// Remove all elements.
    public mutating func removeAll(keepingCapacity keepCapacity: Bool) {
        array.removeAll(keepingCapacity: keepCapacity)
        set.removeAll(keepingCapacity: keepCapacity)
    }

    /// Remove the given element.
    ///
    /// - returns: An element equal to member if member is contained in the set; otherwise, nil.
    @discardableResult
    public mutating func remove(_ element: Element) -> Element? {
        let _removedElement = set.remove(element)
        guard let removedElement = _removedElement else { return nil }

        let idx = array.firstIndex(of: element)!
        array.remove(at: idx)

        return removedElement
    }

    /// Sorts the set according to the ringHash of the elements, taking into account the seed this set was initialized with
    public mutating func sort() {
        let seed = ringHashSeed
        array.sort { $0.ringHash(seed: seed) < $1.ringHash(seed: seed) }
    }

}

extension SortableSet: ExpressibleByArrayLiteral {
    /// Create an instance initialized with `elements`.
    ///
    /// If an element occurs more than once in `element`, only the first one
    /// will be included.
    public init(arrayLiteral elements: Element...) {
        self.init(elements)
    }
}

extension SortableSet: RandomAccessCollection {
    public var startIndex: Int { return contents.startIndex }
    public var endIndex: Int { return contents.endIndex }
    public subscript(index: Int) -> Element {
        return contents[index]
    }
}

enum SortableSetError: Error {
    case notSortedError
}

public func == <T>(lhs: SortableSet<T>, rhs: SortableSet<T>) -> Bool {
    return lhs.contents == rhs.contents
}

extension SortableSet: Hashable where Element: Hashable { }

// TODO find a better name ?
// TODO cache computed hashes and provide method to clear the cache
public protocol RingHashable {
    func ringHash(seed: Int) -> UInt64
}