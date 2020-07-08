import XCTest

#if !canImport(ObjectiveC)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(MembershipViewTests.allTests),
        testCase(MultiNodeCutDetectionTests.allTests),
        testCase(MembershipViewTests.allTests),
        testCase(FastPaxosWithoutFallbackTests.allTests),
        testCase(AdaptiveAccrualFailureDetectorTest.allTests),
        testCase(GrpcMessagingServerTest.allTests),
        testCase(GrpcMessagingClientTest.allTests),
        testCase(AdaptiveAccrualFailureDetectorProviderTests.allTests),
        testCase(MessagingTest.allTests)
    ]
}
#endif
