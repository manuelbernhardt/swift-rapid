import ArgumentParser
import Foundation
#if os(Linux)
import Glibc
#endif

struct Rapid: ParsableCommand {
    static let configuration = CommandConfiguration(
        abstract: "Runs a Rapid cluster",
        subcommands: [Start.self, Join.self],
        defaultSubcommand: Start.self
    )
}

struct Options: ParsableArguments {
    @Option(help: "The host to listen on")
    var host: String = "localhost"

    @Option(help: "The port to listen on")
    var port: Int = 8000
}

extension Rapid {
    struct Start: ParsableCommand {
        @OptionGroup()
        var options: Options

        mutating func run() throws {
            print("Starting Rapid Cluster with listening address \(options.host):\(options.port)")
            try RapidCluster.Builder.with {
                $0.host = options.host
                $0.port = options.port
                $0.registerSubscription(callback: { event in
                    print(event)
                })
            }.start()
            sleep(10000)
        }
    }
    struct Join: ParsableCommand {
        @Argument(help: "The seed host to join")
        var seedHost: String

        @Argument(help: "The seed port to join")
        var seedPort: Int

        @OptionGroup()
        var options: Options

        mutating func run() throws {
            print("Joining the Rapid Cluster at seed \(seedHost):\(seedPort), listening on address \(options.host):\(options.port)")
            let cluster = try RapidCluster.Builder.with {
                $0.host = options.host
                $0.port = options.port
                $0.registerSubscription(callback: { event in
                    print(event)
                })
            }.join(host: seedHost, port: seedPort)
            print("Joined cluster with members \(try! cluster.getMemberList())")
            sleep(10000)
        }

    }
}

Rapid.main()
