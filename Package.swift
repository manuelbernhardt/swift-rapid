// swift-tools-version:5.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "SwiftRapid",
    dependencies: [
        .package(url: "https://github.com/apple/swift-protobuf.git", from: "1.6.0"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"),
        .package(url: "https://github.com/daisuke-t-jp/xxHash-Swift.git", from: "1.0.13"),
        .package(url: "https://github.com/uber/swift-concurrency.git", from: "0.4.0")
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .target(
            name: "SwiftRapid",
            dependencies: ["SwiftProtobuf", "xxHash-Swift", "NIO", "NIOHTTP1", "Concurrency"]),
        .testTarget(
            name: "SwiftRapidTests",
            dependencies: ["SwiftRapid"]),
    ]
)