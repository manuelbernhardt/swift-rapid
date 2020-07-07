import Foundation
import Dispatch
import NIO

public func addressFromParts(_ hostname: String, _ port: Int) -> Endpoint {
    return Endpoint.with {
        $0.hostname = Data(hostname.utf8)
        $0.port = Int32(port)
    }
}

public func nodeIdFromUUID(_ uuid: UUID) -> NodeId {

    var msb: Int64 = 0
    var lsb: Int64 = 0

    // TODO this feels wrong but there's no way to access this tuple via indices (???)
    msb = msb << 8 | ((uuid.uuid.0 & 255) as NSNumber).int64Value
    msb = msb << 8 | ((uuid.uuid.1 & 255) as NSNumber).int64Value
    msb = msb << 8 | ((uuid.uuid.2 & 255) as NSNumber).int64Value
    msb = msb << 8 | ((uuid.uuid.3 & 255) as NSNumber).int64Value
    msb = msb << 8 | ((uuid.uuid.4 & 255) as NSNumber).int64Value
    msb = msb << 8 | ((uuid.uuid.5 & 255) as NSNumber).int64Value
    msb = msb << 8 | ((uuid.uuid.6 & 255) as NSNumber).int64Value
    msb = msb << 8 | ((uuid.uuid.7 & 255) as NSNumber).int64Value

    lsb = lsb << 8 | ((uuid.uuid.8 & 255) as NSNumber).int64Value
    lsb = lsb << 8 | ((uuid.uuid.9 & 255) as NSNumber).int64Value
    lsb = lsb << 8 | ((uuid.uuid.10 & 255) as NSNumber).int64Value
    lsb = lsb << 8 | ((uuid.uuid.11 & 255) as NSNumber).int64Value
    lsb = lsb << 8 | ((uuid.uuid.12 & 255) as NSNumber).int64Value
    lsb = lsb << 8 | ((uuid.uuid.13 & 255) as NSNumber).int64Value
    lsb = lsb << 8 | ((uuid.uuid.14 & 255) as NSNumber).int64Value
    lsb = lsb << 8 | ((uuid.uuid.15 & 255) as NSNumber).int64Value

    return NodeId.with {
        $0.high = msb
        $0.low = lsb
    }
}

func byteArray<T>(from value: T) -> Data where T: FixedWidthInteger {
    return withUnsafePointer(to: value, { ptr in
        Data(bytes: ptr, count: MemoryLayout<T>.size)
    })
}

func require(_ condition: Bool, _ msg: String) throws {
    if (!condition) {
        throw ValidityError.InvalidArgumentError(msg)
    }
}

// TODO is there no such thing in the standard lib?
enum ValidityError: Error {
    case InvalidArgumentError(String)
}

func currentTimeNanos() -> UInt64 {
    return DispatchTime.now().uptimeNanoseconds
}

extension Thread {
    class func printCurrent() {
        print("\(Thread.current)" + " Q: \(OperationQueue.current?.underlyingQueue?.label ?? "None")")
    }
}

public extension NIODeadline {
    func isOverdue() -> Bool {
        self.uptimeNanoseconds < DispatchTime.now().uptimeNanoseconds
    }
}