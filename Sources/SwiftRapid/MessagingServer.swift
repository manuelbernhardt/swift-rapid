import Foundation

/// The server side of the pluggable messaging API
protocol MessagingServer {

    /// Starts the messaging server
    func start() throws

    /// Signals the server that it should free all resources in use
    func shutdown() throws

    /// The messaging server is initialized before the cluster has been joined, because the
    /// node needs the ability to receive messages as part of the join protocol.
    /// Once the node has received a successful join response, the messaging server can start
    /// forwarding incoming messages to the membership service.
    ///
    func onMembershipServiceInitialized(membershipService: MembershipService)

}
