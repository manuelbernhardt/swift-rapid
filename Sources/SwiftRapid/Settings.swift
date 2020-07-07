import Foundation
import NIO

/// Rapid settings
struct Settings {

    let K = 10
    let H = 9
    let L = 4

    var failureDetectorInterval = TimeAmount.seconds(1)

    var consensusFallbackBaseDelay = TimeAmount.seconds(1)

    var messagingClientRequestTimeout = TimeAmount.seconds(5)

    var batchingWindow = TimeAmount.milliseconds(300)
}
