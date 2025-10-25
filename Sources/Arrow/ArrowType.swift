// Copyright 2025 The Apache Software Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import Foundation

public typealias Time32 = Int32
public typealias Time64 = Int64
public typealias Date32 = Int32
public typealias Date64 = Int64
public typealias Timestamp = Int64

public enum ArrowError: Error {
  case none
  case unknownType(String)
  case runtimeError(String)
  case outOfBounds(index: Int64)
  case arrayHasNoElements
  case unknownError(String)
  case notImplemented
  case ioError(String)
  case invalid(String)
}

public enum ArrowTypeId: Sendable {
  case binary
  case boolean
  case date32
  case date64
  case dateType
  case decimal128
  case decimal256
  case dictionary
  case double
  case fixedSizeBinary
  case fixedWidthType
  case float
  // case HalfFloatType
  case int16
  case int32
  case int64
  case int8
  case integer
  case intervalUnit
  case list
  case nested
  case null
  case number
  case string
  case strct
  case time32
  case time64
  case timestamp
  case time
  case uint16
  case uint32
  case uint64
  case uint8
  case union
  case unknown
}

public enum ArrowTime32Unit {
  case seconds
  case milliseconds
}

public enum ArrowTime64Unit {
  case microseconds
  case nanoseconds
}

public class ArrowTypeTime32: ArrowType {
  let unit: ArrowTime32Unit
  public init(_ unit: ArrowTime32Unit) {
    self.unit = unit
    super.init(ArrowType.arrowTime32)
  }

  public override var cDataFormatId: String {
    get throws {
      switch self.unit {
      case .milliseconds:
        return "ttm"
      case .seconds:
        return "tts"
      }
    }
  }
}

public class ArrowTypeTime64: ArrowType {
  let unit: ArrowTime64Unit
  public init(_ unit: ArrowTime64Unit) {
    self.unit = unit
    super.init(ArrowType.arrowTime64)
  }

  public override var cDataFormatId: String {
    get throws {
      switch self.unit {
      case .microseconds:
        return "ttu"
      case .nanoseconds:
        return "ttn"
      }
    }
  }
}

public enum ArrowTimestampUnit {
  case seconds
  case milliseconds
  case microseconds
  case nanoseconds
}

public class ArrowTypeTimestamp: ArrowType {
  let unit: ArrowTimestampUnit
  let timezone: String?

  public init(_ unit: ArrowTimestampUnit, timezone: String? = nil) {
    self.unit = unit
    self.timezone = timezone

    super.init(ArrowType.arrowTimestamp)
  }

  public convenience init(type: ArrowTypeId) {
    self.init(.milliseconds, timezone: nil)
  }

  public override var cDataFormatId: String {
    get throws {
      let unitChar: String
      switch self.unit {
      case .seconds: unitChar = "s"
      case .milliseconds: unitChar = "m"
      case .microseconds: unitChar = "u"
      case .nanoseconds: unitChar = "n"
      }

      if let timezone = self.timezone {
        return "ts\(unitChar):\(timezone)"
      } else {
        return "ts\(unitChar)"
      }
    }
  }
}

public class ArrowTypeStruct: ArrowType {
  let fields: [ArrowField]
  public init(_ info: ArrowType.Info, fields: [ArrowField]) {
    self.fields = fields
    super.init(info)
  }
}

public class ArrowTypeList: ArrowType {
  let elementType: ArrowType

  public init(_ elementType: ArrowType) {
    self.elementType = elementType
    super.init(ArrowType.arrowList)
  }
}

public class ArrowType {
  public private(set) var info: ArrowType.Info
  public static let arrowInt8 = Info.primitiveInfo(ArrowTypeId.int8)
  public static let arrowInt16 = Info.primitiveInfo(ArrowTypeId.int16)
  public static let arrowInt32 = Info.primitiveInfo(ArrowTypeId.int32)
  public static let arrowInt64 = Info.primitiveInfo(ArrowTypeId.int64)
  public static let arrowUInt8 = Info.primitiveInfo(ArrowTypeId.uint8)
  public static let arrowUInt16 = Info.primitiveInfo(ArrowTypeId.uint16)
  public static let arrowUInt32 = Info.primitiveInfo(ArrowTypeId.uint32)
  public static let arrowUInt64 = Info.primitiveInfo(ArrowTypeId.uint64)
  public static let arrowFloat = Info.primitiveInfo(ArrowTypeId.float)
  public static let arrowDouble = Info.primitiveInfo(ArrowTypeId.double)
  public static let arrowUnknown = Info.primitiveInfo(ArrowTypeId.unknown)
  public static let arrowString = Info.variableInfo(ArrowTypeId.string)
  public static let arrowBool = Info.primitiveInfo(ArrowTypeId.boolean)
  public static let arrowDate32 = Info.primitiveInfo(ArrowTypeId.date32)
  public static let arrowDate64 = Info.primitiveInfo(ArrowTypeId.date64)
  public static let arrowBinary = Info.variableInfo(ArrowTypeId.binary)
  public static let arrowTime32 = Info.timeInfo(ArrowTypeId.time32)
  public static let arrowTime64 = Info.timeInfo(ArrowTypeId.time64)
  public static let arrowTimestamp = Info.timeInfo(ArrowTypeId.timestamp)
  public static let arrowStruct = Info.complexInfo(ArrowTypeId.strct)
  public static let arrowList = Info.complexInfo(ArrowTypeId.list)

  public init(_ info: ArrowType.Info) {
    self.info = info
  }

  public var id: ArrowTypeId {
    switch self.info {
    case .primitiveInfo(let id):
      return id
    case .timeInfo(let id):
      return id
    case .variableInfo(let id):
      return id
    case .complexInfo(let id):
      return id
    }
  }

  public enum Info: Sendable {
    case primitiveInfo(ArrowTypeId)
    case variableInfo(ArrowTypeId)
    case timeInfo(ArrowTypeId)
    case complexInfo(ArrowTypeId)
  }

  public static func infoForType(  // swiftlint:disable:this cyclomatic_complexity
    _ type: Any.Type
  ) -> ArrowType.Info {
    if type == String.self {
      return ArrowType.arrowString
    } else if type == Date.self {
      return ArrowType.arrowDate64
    } else if type == Bool.self {
      return ArrowType.arrowBool
    } else if type == Data.self {
      return ArrowType.arrowBinary
    } else if type == Int8.self {
      return ArrowType.arrowInt8
    } else if type == Int16.self {
      return ArrowType.arrowInt16
    } else if type == Int32.self {
      return ArrowType.arrowInt32
    } else if type == Int64.self {
      return ArrowType.arrowInt64
    } else if type == UInt8.self {
      return ArrowType.arrowUInt8
    } else if type == UInt16.self {
      return ArrowType.arrowUInt16
    } else if type == UInt32.self {
      return ArrowType.arrowUInt32
    } else if type == UInt64.self {
      return ArrowType.arrowUInt64
    } else if type == Float.self {
      return ArrowType.arrowFloat
    } else if type == Double.self {
      return ArrowType.arrowDouble
    } else {
      return ArrowType.arrowUnknown
    }
  }

  public static func infoForNumericType<T>(_ type: T.Type) -> ArrowType.Info {
    if type == Int8.self {
      return ArrowType.arrowInt8
    } else if type == Int16.self {
      return ArrowType.arrowInt16
    } else if type == Int32.self {
      return ArrowType.arrowInt32
    } else if type == Int64.self {
      return ArrowType.arrowInt64
    } else if type == UInt8.self {
      return ArrowType.arrowUInt8
    } else if type == UInt16.self {
      return ArrowType.arrowUInt16
    } else if type == UInt32.self {
      return ArrowType.arrowUInt32
    } else if type == UInt64.self {
      return ArrowType.arrowUInt64
    } else if type == Float.self {
      return ArrowType.arrowFloat
    } else if type == Double.self {
      return ArrowType.arrowDouble
    } else {
      return ArrowType.arrowUnknown
    }
  }

  public func getStride(
    ) -> Int
  {
    switch self.id {
    case .int8:
      return MemoryLayout<Int8>.stride
    case .int16:
      return MemoryLayout<Int16>.stride
    case .int32:
      return MemoryLayout<Int32>.stride
    case .int64:
      return MemoryLayout<Int64>.stride
    case .uint8:
      return MemoryLayout<UInt8>.stride
    case .uint16:
      return MemoryLayout<UInt16>.stride
    case .uint32:
      return MemoryLayout<UInt32>.stride
    case .uint64:
      return MemoryLayout<UInt64>.stride
    case .float:
      return MemoryLayout<Float>.stride
    case .double:
      return MemoryLayout<Double>.stride
    case .boolean:
      return MemoryLayout<Bool>.stride
    case .date32:
      return MemoryLayout<Date32>.stride
    case .date64:
      return MemoryLayout<Date64>.stride
    case .time32:
      return MemoryLayout<Time32>.stride
    case .time64:
      return MemoryLayout<Time64>.stride
    case .timestamp:
      return MemoryLayout<Timestamp>.stride
    case .binary:
      return MemoryLayout<Int8>.stride
    case .string:
      return MemoryLayout<Int8>.stride
    case .strct, .list:
      return 0
    default:
      fatalError("Stride requested for unknown type: \(self)")
    }
  }

  public var cDataFormatId: String {
    get throws {
      switch self.id {
      case ArrowTypeId.int8:
        return "c"
      case ArrowTypeId.int16:
        return "s"
      case ArrowTypeId.int32:
        return "i"
      case ArrowTypeId.int64:
        return "l"
      case ArrowTypeId.uint8:
        return "C"
      case ArrowTypeId.uint16:
        return "S"
      case ArrowTypeId.uint32:
        return "I"
      case ArrowTypeId.uint64:
        return "L"
      case ArrowTypeId.float:
        return "f"
      case ArrowTypeId.double:
        return "g"
      case ArrowTypeId.boolean:
        return "b"
      case ArrowTypeId.date32:
        return "tdD"
      case ArrowTypeId.date64:
        return "tdm"
      case ArrowTypeId.time32:
        if let time32 = self as? ArrowTypeTime32 {
          return try time32.cDataFormatId
        }
        return "tts"
      case ArrowTypeId.time64:
        if let time64 = self as? ArrowTypeTime64 {
          return try time64.cDataFormatId
        }
        return "ttu"
      case ArrowTypeId.timestamp:
        if let timestamp = self as? ArrowTypeTimestamp {
          return try timestamp.cDataFormatId
        }
        return "tsu"
      case ArrowTypeId.binary:
        return "z"
      case ArrowTypeId.string:
        return "u"
      case ArrowTypeId.strct:
        if let structType = self as? ArrowTypeStruct {
          var format = "+s"
          for field in structType.fields {
            format += try field.type.cDataFormatId
          }
          return format
        }
        throw ArrowError.invalid("Invalid struct type")
      case ArrowTypeId.list:
        if let listType = self as? ArrowTypeList {
          return "+l" + (try listType.elementType.cDataFormatId)
        }
        throw ArrowError.invalid("Invalid list type")
      default:
        throw ArrowError.notImplemented
      }
    }
  }

  public static func fromCDataFormatId(  // swiftlint:disable:this cyclomatic_complexity
    _ from: String
  ) throws -> ArrowType {
    if from == "c" {
      return ArrowType(ArrowType.arrowInt8)
    } else if from == "s" {
      return ArrowType(ArrowType.arrowInt16)
    } else if from == "i" {
      return ArrowType(ArrowType.arrowInt32)
    } else if from == "l" {
      return ArrowType(ArrowType.arrowInt64)
    } else if from == "C" {
      return ArrowType(ArrowType.arrowUInt8)
    } else if from == "S" {
      return ArrowType(ArrowType.arrowUInt16)
    } else if from == "I" {
      return ArrowType(ArrowType.arrowUInt32)
    } else if from == "L" {
      return ArrowType(ArrowType.arrowUInt64)
    } else if from == "f" {
      return ArrowType(ArrowType.arrowFloat)
    } else if from == "g" {
      return ArrowType(ArrowType.arrowDouble)
    } else if from == "b" {
      return ArrowType(ArrowType.arrowBool)
    } else if from == "tdD" {
      return ArrowType(ArrowType.arrowDate32)
    } else if from == "tdm" {
      return ArrowType(ArrowType.arrowDate64)
    } else if from == "tts" {
      return ArrowTypeTime32(.seconds)
    } else if from == "ttm" {
      return ArrowTypeTime32(.milliseconds)
    } else if from == "ttu" {
      return ArrowTypeTime64(.microseconds)
    } else if from == "ttn" {
      return ArrowTypeTime64(.nanoseconds)
    } else if from.starts(with: "ts") {
      let components = from.split(separator: ":", maxSplits: 1)
      guard let unitPart = components.first, unitPart.count == 3 else {
        throw ArrowError.invalid(
          "Invalid timestamp format '\(from)'. Expected format 'ts[s|m|u|n][:timezone]'")
      }

      let unitChar = unitPart.suffix(1)
      let unit: ArrowTimestampUnit
      switch unitChar {
      case "s": unit = .seconds
      case "m": unit = .milliseconds
      case "u": unit = .microseconds
      case "n": unit = .nanoseconds
      default:
        throw ArrowError.invalid(
          "Unrecognized timestamp unit '\(unitChar)'. Expected 's', 'm', 'u', or 'n'.")
      }

      let timezone = components.count > 1 ? String(components[1]) : nil
      return ArrowTypeTimestamp(unit, timezone: timezone)
    } else if from == "z" {
      return ArrowType(ArrowType.arrowBinary)
    } else if from == "u" {
      return ArrowType(ArrowType.arrowString)
    }

    throw ArrowError.notImplemented
  }
}

extension ArrowType.Info: Equatable {
  public static func == (lhs: ArrowType.Info, rhs: ArrowType.Info) -> Bool {
    switch (lhs, rhs) {
    case (.primitiveInfo(let lhsId), .primitiveInfo(let rhsId)):
      return lhsId == rhsId
    case (.variableInfo(let lhsId), .variableInfo(let rhsId)):
      return lhsId == rhsId
    case (.timeInfo(let lhsId), .timeInfo(let rhsId)):
      return lhsId == rhsId
    case (.complexInfo(let lhsId), .complexInfo(let rhsId)):
      return lhsId == rhsId
    default:
      return false
    }
  }
}

func getBytesFor<T>(_ data: T) -> Data? {
  if let temp = data as? String {
    return temp.data(using: .utf8)
  } else if T.self == Data.self {
    return data as? Data
  } else {
    return nil
  }
}
// swiftlint:disable:this file_length
