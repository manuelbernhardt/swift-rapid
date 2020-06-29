import NIO
import NIOConcurrencyHelpers
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

                var wasFailureSignaled = false
                func signalFailure() -> EventLoopFuture<()> {
                    wasFailureSignaled = true
                    return serverGroup!.next().makeSucceededFuture(())
                }

                let fd = try! provider.createInstance(subject: subjectAddress, signalFailure: signalFailure)

                for _ in 0..<10 {
                    let _ = fd()
                    XCTAssertFalse(wasFailureSignaled)
                    usleep(500000)

                }
            })
        }
    }

    func testDelayedHeartbeats() throws {
        let address = addressFromParts("localhost", 8000)
        let subjectAddress = addressFromParts("localhost", 8090)
        let probeMembershipService = ProbeMembershipService(el: serverGroup!.next())

        withTestClient { client in
            withTestServer(subjectAddress, { (subjectServer: TestMessagingServer) in
                subjectServer.onMembershipServiceInitialized(membershipService: probeMembershipService)

                let provider = AdaptiveAccrualFailureDetectorProvider(selfAddress: address, messagingClient: client, el: clientGroup!.next())

                var failureCount = 0
                func signalFailure() -> EventLoopFuture<()> {
                    failureCount += 1
                    return serverGroup!.next().makeSucceededFuture(())
                }

                let fd = try! provider.createInstance(subject: subjectAddress, signalFailure: signalFailure)

                for _ in 0..<5 {
                    let _ = fd()
                    usleep(600000)
                }
                XCTAssertEqual(0, failureCount)
                probeMembershipService.setDelay(delay: 1500000)
                for _ in 0..<6 {
                    let _ = fd()
                    usleep(600000)
                }
                // 5 delayed heartbeats, accrual FD starts suspecting after 1 beat
                XCTAssertEqual(4, failureCount)

                // give the chance to the last heartbeat to come in before the test shuts down all event loops
                sleep(2)
            })
        }
    }


    class ProbeMembershipService: TestMembershipService {
        private let lock = Lock()
        private var delay: UInt32 = 0
        let probeSuccess = RapidResponse.with({
            $0.probeResponse = ProbeResponse.with({
                $0.status = NodeStatus.ok
            })
        })

        func setDelay(delay: UInt32) {
            lock.withLock {
                self.delay = delay
            }
        }

        override func handleRequest(request: RapidRequest) -> EventLoopFuture<RapidResponse> {
            lock.withLock {
                usleep(delay)
                return el.makeSucceededFuture(probeSuccess)
            }
        }
    }


    static var allTests = [
        ("testSuccessfulHeartbeats", testSuccessfulHeartbeats),
        ("testDelayedHeartbeats", testDelayedHeartbeats)
    ]

}
