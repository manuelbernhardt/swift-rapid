import NIO
import XCTest
@testable import SwiftRapid

class AdaptiveAccrualFailureDetectorProviderTests: XCTestCase, TestServerMessaging, TestClientMessaging {

    var serverGroup: MultiThreadedEventLoopGroup? = nil
    var clientGroup: MultiThreadedEventLoopGroup? = nil
    var clientSettings: Settings = Settings()

    override func setUp() {
        serverGroup = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        clientGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        clientSettings = Settings()
    }

    override func tearDown() {
        try! serverGroup?.syncShutdownGracefully()
        try! clientGroup?.syncShutdownGracefully()
    }

    func testSuccessfulHeartbeats() throws {
        let address = addressFromParts("localhost", 8000)
        let subjectAddress = addressFromParts("localhost", 8090)

        withTestClient { client in
            withTestServer(subjectAddress, { (subjectServer: TestMessagingServer) in
                subjectServer.onMembershipServiceInitialized(membershipService: ProbeMembershipService(el: serverGroup!.next()))

                let provider = AdaptiveAccrualFailureDetectorProvider(selfAddress: address, messagingClient: client, el: clientGroup!.next())

                let fd: () -> EventLoopFuture<FailureDetectionResult> = try! provider.createInstance(subject: subjectAddress)

                for _ in 0..<10 {
                    let result = try! fd().wait()
                    XCTAssertEqual(result, FailureDetectionResult.success)
                    usleep(500000)

                }
            })
        }
    }

    class ProbeMembershipService: TestMembershipService {
        let probeSuccess = RapidResponse.with({
            $0.probeResponse = ProbeResponse.with({
                $0.status = NodeStatus.ok
            })
        })
        override func handleRequest(request: RapidRequest) -> EventLoopFuture<RapidResponse> {
            return el.makeSucceededFuture(probeSuccess)
        }
    }


    static var allTests = [
        ("testSuccessfulHeartbeats", testSuccessfulHeartbeats)
    ]

}
