import Foundation
import NIO

/// A provider for edge failure detectors that monitor one edge of the expander graph.
///
/// Implementations are expected to be invoked periodically.
///
/// On every configuration change, the membership service invokes createInstance for each edge to be monitored by this node
///
/// TODO extend API to include notification facility from the MultiNodeCutDetector, to allow to fast-track partition detection
public protocol EdgeFailureDetectorProvider {

    /// Creates a new failure detector instance
    /// - Parameters:
    ///   - subject: the subject to monitor
    ///   - signalFailure: a callback to call when the failure detector detects a failure. This should only be invoked one time.
    /// - Returns: An EventLoopFuture with a void result
    func createInstance(subject: Endpoint, signalFailure: @escaping (Endpoint) -> ()) throws -> () -> EventLoopFuture<()>

}