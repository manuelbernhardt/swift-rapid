import Foundation
import NIO

/// A provider for edge failure detectors that monitor one edge of the expander graph.
///
/// Implementations are expected to be invoked periodically.
///
/// On every configuration change, the membership service invokes createInstance for each edge to be monitored by this node
///
/// TODO extend API to include notification facility from the MultiNodeCutDetector, to allow to fast-track partition detection
protocol EdgeFailureDetectorProvider {

    func createInstance(subject: Endpoint, signalFailure: @escaping (Endpoint) -> ()) throws -> () -> EventLoopFuture<()>

}