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

    func shutdown(el: EventLoop) -> EventLoopFuture<Void> {
        server.map {
            $0.close()
        } ?? el.makeSucceededFuture(())
    }

    func onMembershipServiceInitialized(membershipService: MembershipService) {
        self.membershipService = membershipService
    }

    private let probeResponse = RapidResponse.with {
        $0.probeResponse = ProbeResponse()
    }

    private let bootstrappingProbeResponse = RapidResponse.with {
        $0.probeResponse = ProbeResponse.with {
            $0.status = NodeStatus.bootstrapping
        }
    }

    func sendRequest(request: RapidRequest, context: StatusOnlyCallContext) -> EventLoopFuture<RapidResponse> {
        if let service = membershipService {
            return service.handleRequest(request: request)
        } else {
            return context.eventLoop.makeSucceededFuture(RapidResponse())
        }
    }
}
