import Foundation
import NIO

/// Rapid settings
public struct Settings {

    let K = 10
    let H = 9
    let L = 4

    var failureDetectorInterval = TimeAmount.seconds(5)

    var consensusFallbackBaseDelay = TimeAmount.seconds(10)

    var messagingClientDefaultRequestTimeout = TimeAmount.seconds(5)
    var messagingClientJoinRequestTimeout = TimeAmount.seconds(10)
    var messagingClientProbeRequestTimeout = TimeAmount.seconds(1)

    var batchingWindow = TimeAmount.milliseconds(100)

    var joinDelaySeconds = 5
    var joinAttempts = 10
}
