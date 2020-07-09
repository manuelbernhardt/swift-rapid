import Foundation
import NIO
import GRPC

class GrpcMessagingServer: MessagingServer, MembershipServiceProvider {

    private let selfAddress: Endpoint
    private let group: MultiThreadedEventLoopGroup
    private var server: Server? = nil
    private var membershipService: MembershipService? = nil

    init(address: Endpoint, group: MultiThreadedEventLoopGroup) {
        self.selfAddress = address
        self.group = group
    }

    func start() throws {
        let _ = Server
            .insecure(group: group)
            .withServiceProviders([self])
            .bind(host: String(decoding: selfAddress.hostname, as: UTF8.self), port: Int(selfAddress.port))
            .map {
                self.server = $0
            }
    }

    func shutdown() throws {
        try server.map {
            try $0.close().wait()
        }
    }

    func onMembershipServiceInitialized(membershipService: MembershipService) {
        self.membershipService = membershipService
    }

    func sendRequest(request: RapidRequest, context: StatusOnlyCallContext) -> EventLoopFuture<RapidResponse> {
        if let service = membershipService {
            return service.handleRequest(request: request)
        } else {
            switch request.content {
                case .probeMessage:
                    let response = RapidResponse.with {
                        $0.probeResponse = ProbeResponse.with {
                            $0.status = NodeStatus.bootstrapping
                        }
                    }
                    return context.eventLoop.makeSucceededFuture(response)
                default:
                    return context.eventLoop.makeSucceededFuture(RapidResponse())
            }
        }
    }
}
