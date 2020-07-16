import NIO
import NIOConcurrencyHelpers
import XCTest
import Foundation
@testable import SwiftRapid

/// Note: heartbeat timing is tuned so that this suite runs well on the CI
class AdaptiveAccrualFailureDetectorProviderTests: XCTestCase, TestServerMessaging, TestClientMessaging {

    var eventLoopGroup: MultiThreadedEventLoopGroup? = nil
    var clientSettings: Settings = Settings()

    override func setUp() {
        eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 4)
        clientSettings = Settings()
    }

    override func tearDown() {
        try! eventLoopGroup?.syncShutdownGracefully()
    }

    func testSuccessfulHeartbeats() throws {
        let provider = ActorRefProvider(group: eventLoopGroup!)
        let address = addressFromParts("localhost", 8000)
        let subjectAddress = addressFromParts("localhost", 8090)

        withTestClient { client in
            withTestServer(subjectAddress, { (subjectServer: TestMessagingServer) in
                subjectServer.onMembershipServiceInitialized(membershipService: ProbeMembershipService(el: eventLoopGroup!.next()))

                let provider = AdaptiveAccrualFailureDetectorProvider(selfEndpoint: address, messagingClient: client, provider: provider, el: eventLoopGroup!.next())

                var wasFailureSignaled = false
                func signalFailure(endpoint: Endpoint) {
                    wasFailureSignaled = true
                }

                let fd = try! provider.createInstance(subject: subjectAddress, signalFailure: signalFailure)

                for _ in 0..<10 {
                    let _ = fd()
                    XCTAssertFalse(wasFailureSignaled)
                    Thread.sleep(forTimeInterval: 0.5)
                }
            })
        }
    }

    func testDelayedHeartbeats() throws {
        let provider = ActorRefProvider(group: eventLoopGroup!)

        let address = addressFromParts("localhost", 8000)
        let subjectAddress = addressFromParts("localhost", 8090)
        let probeMembershipService = ProbeMembershipService(el: eventLoopGroup!.next())

        withTestClient { client in
            withTestServer(subjectAddress, { (subjectServer: TestMessagingServer) in
                subjectServer.onMembershipServiceInitialized(membershipService: probeMembershipService)

                let provider = AdaptiveAccrualFailureDetectorProvider(selfEndpoint: address, messagingClient: client, provider: provider, el: eventLoopGroup!.next())

                let failureCount = NIOAtomic.makeAtomic(value: 0)
                func signalFailure(endpoint: Endpoint) -> () {
                    failureCount.add(1)
                }

                let fd = try! provider.createInstance(subject: subjectAddress, signalFailure: signalFailure)

                for _ in 0..<5 {
                    let _ = fd()
                    Thread.sleep(forTimeInterval: 0.5)
                }
                XCTAssertEqual(0, failureCount.load())
                probeMembershipService.setDelay(delay: 2000000)
                for _ in 0..<6 {
                    let _ = fd()
                    Thread.sleep(forTimeInterval: 0.5)
                }
                // the failure detector should report only one failure
                XCTAssertEqual(1, failureCount.load())
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
