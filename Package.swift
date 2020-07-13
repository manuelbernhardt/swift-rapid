// swift-tools-version:5.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "SwiftRapid",
    products: [
        .executable(name: "SwiftRapid", targets: ["SwiftRapid"]),
    ],
    dependencies: [
        .package(name: "SwiftProtobuf", url: "https://github.com/apple/swift-protobuf.git", from: "1.6.0"),
        .package(url: "https://github.com/grpc/grpc-swift", from: "1.0.0-alpha.12"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"),
        .package(url: "https://github.com/apple/swift-argument-parser", from: "0.2.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
        .package(url: "https://github.com/daisuke-t-jp/xxHash-Swift.git", from: "1.0.13"),
        .package(url: "https://github.com/swift-server/swift-backtrace.git", from: "1.2.0"),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .target(
            name: "SwiftRapid",
            dependencies: ["SwiftProtobuf", "xxHash-Swift", .product(name: "Logging", package: "swift-log"), .product(name: "Backtrace", package: "swift-backtrace"), .product(name: "NIO", package: "swift-nio"), .product(name: "GRPC", package: "grpc-swift"), .product(name: "ArgumentParser", package: "swift-argument-parser") ]),
        .testTarget(
            name: "SwiftRapidTests",
            dependencies: ["SwiftRapid"]),
    ]
)