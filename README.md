# swift-rapid

[Swift](https://swift.org/) implementation of the [Rapid Membership Protocol](https://www.usenix.org/conference/atc18/presentation/suresh). It is inspired by the [Java implementation](https://github.com/lalithsuresh/rapid).

This is a proof-of-concept / playground project and is not meant for production use as yet.

A few notes about this implementation:

- it uses GRPC as a communication layer. There are likely more efficient ways to go about it than full-blown GRPC. The communication layer is pluggable, see [MessagingClient](https://github.com/manuelbernhardt/swift-rapid/blob/master/Sources/SwiftRapid/MessagingClient.swift) and [MessagingServer](https://github.com/manuelbernhardt/swift-rapid/blob/master/Sources/SwiftRapid/MessagingServer.swift)
- the edge failure detector is pluggable as well, see [EdgeFailureDetectorProvider](https://github.com/manuelbernhardt/swift-rapid/blob/master/Sources/SwiftRapid/EdgeFailureDetectorProvider.swift)
  - the default failure detector implementation is the the [New Adaptive Accrual Failure Detector](https://www.semanticscholar.org/paper/A-new-adaptive-accrual-failure-detector-for-systems-Satzger-Pietzowski/8805d522cd6cef723aae55595f918e09914e4316?p2df) which was chosen here simply because it is easier to implement than the Phi Accrual Failure Detector. Read more about it [here](https://manuel.bernhardt.io/2017/07/26/a-new-adaptive-accrual-failure-detector-for-akka/)
- at the time of writing this there aren't many established concurrency primitives in Swift so this project uses a few of them creatively. It makes use of:
  - [swift-nio](https://github.com/apple/swift-nio) and its `EventLoopFuture`
  - the good old [DispatchQueue](https://developer.apple.com/documentation/dispatch/dispatchqueue)
  - [swift-nio concurreny helpers](https://apple.github.io/swift-nio/docs/current/NIOConcurrencyHelpers/index.html), mainly `NIOAtomic` and `Lock`
  - a home-grown, stupid, horrible [actor implementation](https://github.com/manuelbernhardt/swift-rapid/blob/master/Sources/SwiftRapid/Actor.swift) (but hey, it works ¯\_(ツ)_/¯ )
- at the core of it all is the [RapidStateMachine](https://github.com/manuelbernhardt/swift-rapid/blob/master/Sources/SwiftRapid/RapidStateMachine.swift) which follows the pattern outlined in [this talk](https://www.youtube.com/watch?v=7UC7OUdtY_Q). The most interesting states are `active` and `viewChanging`. A lot of care is put into stashing/keeping alerts for "future" configurations in order to honor the virtual synchrony requirements of Rapid and yet at the same time to allow for progress in settings where some nodes are faster than others (see [this article](https://manuel.bernhardt.io/2020/04/30/10000-node-cluster-with-akka-and-rapid/) on a very large cluster where this happens)

## Building

- run `./generateProtos.sh` to generate the message protocol with [swift-protobuf](https://github.com/apple/swift-protobuf). This script expects `protoc` to be available on the path.
- run `swift test` to verify that the tests are passing. You may need to increase the maximum number of allowed open files on your system (using e.g. `ulimit -n 20000` on linux).
- run `swift build` to build the binaries

## Running via the CLI

To assemble a cluster for testing puropses, you can use the `SwiftRapidCLI` interface:

- run `./SwiftRapidCLI start` (under `.build/<yourPlatform>/debug`) to start a seed node on `localhost:8000`
- run `./SwiftRapidCLI join localhost 8000 --port 8001` to join the cluster and listen on `localhost:8001`
- note: for Paxos to be able to reach consensus you need at least 3 nodes, so make sure that you e.g. run at least a 5 port cluster

## Running via the API

The entry point for this project is the [RapidCluster](https://github.com/manuelbernhardt/swift-rapid/blob/master/Sources/SwiftRapid/RapidCluster.swift) class.

In order to start a seed node, use the `start` method:

```swift
 try RapidCluster.Builder.with {
    $0.host = "localhost"
    $0.port = 8000
    $0.registerSubscription(callback: { event in
        // print out any event on stdout
        print(event)
    })
}.start()
```

In order to join a seed node, use the `join` method:

```swift
let cluster = try RapidCluster.Builder.with {
    $0.host = "localhost"
    $0.port = 8001
    $0.registerSubscription(callback: { event in
        print(event)
    })
}.join(host: "localhost", port: 8000)

// print all members
print(try cluster.getMemberList())
```