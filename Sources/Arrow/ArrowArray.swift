// Copyright 2025 The Apache Software Foundation
// Copyright 2025 The Columnar Swift Contributors
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

/// A type-erased ArrowArray.
public protocol AnyArrowArray {
  var type: ArrowType { get }
  var length: UInt { get }
  var nullCount: UInt { get }
  var arrowData: ArrowData { get }
  var bufferData: [Data] { get }
  var bufferDataSizes: [Int] { get }
  func asAny(_ index: UInt) -> Any?
  func asString(_ index: UInt) -> String
}

// MARK: - Core Protocol

/// The interface for Arrow array types.
public protocol ArrowArray<ItemType>: AnyArrowArray {
  associatedtype ItemType
  var arrowData: ArrowData { get }
  init(_ arrowData: ArrowData) throws(ArrowError)
  subscript(_ index: UInt) -> ItemType? { get }
}

// MARK: - Default Implementations
extension ArrowArray {
  public var nullCount: UInt {
    arrowData.nullCount
  }

  public var length: UInt {
    arrowData.length
  }

  public var type: ArrowType {
    arrowData.type
  }

  public var bufferData: [Data] {
    arrowData.buffers.map { buffer in
      var data = Data()
      buffer.append(to: &data)
      return data
    }
  }

  public var bufferDataSizes: [Int] {
    arrowData.buffers.map { Int($0.capacity) }
  }

  public func isNull(at index: UInt) throws -> Bool {
    if index >= self.length {
      throw ArrowError.outOfBounds(index: Int64(index))
    }
    return arrowData.isNull(index)
  }

  public func asString(_ index: UInt) -> String {
    guard let value = self[index] else {
      return ""
    }
    return "\(value)"
  }

  public func asAny(_ index: UInt) -> Any? {
    self[index]
  }
}

// MARK: Fixed Arrays

public protocol FixedArrayProtocol: ArrowArray where ItemType: BitwiseCopyable {
}

extension FixedArrayProtocol {
  public subscript(_ index: UInt) -> ItemType? {
    if arrowData.isNull(index) {
      return nil
    }
    let byteOffset = arrowData.stride * Int(index)

    // FIXME: Can probably do this and remove BitwiseCopyable constraint.
    //    let buffer = UnsafeBufferPointer<ItemType>(
    //      start: arrowData.buffers[1].rawPointer.assumingMemoryBound(to: ItemType.self),
    //      count: Int(arrowData.length)
    //    )
    //    return buffer[Int(index)]
    return arrowData.buffers[1].rawPointer
      .advanced(by: byteOffset)
      .load(as: ItemType.self)
  }
}

public struct FixedArray<T>: FixedArrayProtocol where T: BitwiseCopyable {
  public typealias ItemType = T
  public let arrowData: ArrowData

  public init(arrowData: ArrowData) {
    self.arrowData = arrowData
  }

  public init(_ arrowData: ArrowData) {
    self.arrowData = arrowData
  }
}

public struct StringArray: ArrowArray {
  public typealias ItemType = String
  public let arrowData: ArrowData

  public init(_ arrowData: ArrowData) {
    self.arrowData = arrowData
  }

  public subscript(_ index: UInt) -> String? {
    let offsetIndex = MemoryLayout<Int32>.stride * Int(index)
    if self.arrowData.isNull(index) {
      return nil
    }

    let offsets = self.arrowData.buffers[1]
    let values = self.arrowData.buffers[2]

    var startIndex: Int32 = 0
    if index > 0 {
      startIndex = offsets.rawPointer.advanced(by: offsetIndex).load(
        as: Int32.self)
    }

    let endIndex = offsets.rawPointer.advanced(
      by: offsetIndex + MemoryLayout<Int32>.stride
    )
    .load(as: Int32.self)
    let arrayLength = Int(endIndex - startIndex)
    let rawPointer = values.rawPointer.advanced(by: Int(startIndex))
      .bindMemory(to: UInt8.self, capacity: arrayLength)
    let buffer = UnsafeBufferPointer<UInt8>(
      start: rawPointer, count: arrayLength)
    let byteArray = Array(buffer)
    return String(data: Data(byteArray), encoding: .utf8)
  }
}

public struct BoolArray: ArrowArray {
  public typealias ItemType = Bool
  public let arrowData: ArrowData

  public init(_ arrowData: ArrowData) {
    self.arrowData = arrowData
  }

  public subscript(_ index: UInt) -> Bool? {
    if self.arrowData.isNull(index) {
      return nil
    }
    let valueBuffer = self.arrowData.buffers[1]
    return BitUtility.isSet(index, buffer: valueBuffer)
  }
}

public struct Date32Array: ArrowArray {
  public typealias ItemType = Date
  public let arrowData: ArrowData

  public init(_ arrowData: ArrowData) {
    self.arrowData = arrowData
  }

  public subscript(_ index: UInt) -> Date? {
    if self.arrowData.isNull(index) {
      return nil
    }
    let byteOffset = self.arrowData.stride * Int(index)
    let milliseconds = self.arrowData.buffers[1].rawPointer.advanced(
      by: byteOffset
    ).load(as: UInt32.self)
    return Date(timeIntervalSince1970: TimeInterval(milliseconds * 86400))
  }
}

public struct Date64Array: ArrowArray {
  public typealias ItemType = Date
  public let arrowData: ArrowData

  public init(_ arrowData: ArrowData) {
    self.arrowData = arrowData
  }

  public subscript(_ index: UInt) -> Date? {
    if self.arrowData.isNull(index) {
      return nil
    }
    let byteOffset = self.arrowData.stride * Int(index)
    let milliseconds = self.arrowData.buffers[1].rawPointer.advanced(
      by: byteOffset
    ).load(as: UInt64.self)
    return Date(timeIntervalSince1970: TimeInterval(milliseconds / 1000))
  }
}

public typealias Time64Array = FixedArray<Time64>

public typealias Time32Array = FixedArray<Time32>

public struct TimestampArray: FixedArrayProtocol {
  public typealias ItemType = Timestamp
  public let arrowData: ArrowData

  public init(_ arrowData: ArrowData) {
    self.arrowData = arrowData
  }

  public struct FormattingOptions: Equatable {
    public var dateFormat: String = "yyyy-MM-dd HH:mm:ss.SSS"
    public var locale: Locale = .current
    public var includeTimezone: Bool = true
    public var fallbackToRaw: Bool = true

    public init(
      dateFormat: String = "yyyy-MM-dd HH:mm:ss.SSS",
      locale: Locale = .current,
      includeTimezone: Bool = true,
      fallbackToRaw: Bool = true
    ) {
      self.dateFormat = dateFormat
      self.locale = locale
      self.includeTimezone = includeTimezone
      self.fallbackToRaw = fallbackToRaw
    }

    public static func == (lhs: FormattingOptions, rhs: FormattingOptions)
      -> Bool
    {
      lhs.dateFormat == rhs.dateFormat
        && lhs.locale.identifier == rhs.locale.identifier
        && lhs.includeTimezone == rhs.includeTimezone
        && lhs.fallbackToRaw == rhs.fallbackToRaw
    }
  }

  private var cachedFormatter: DateFormatter?
  private var cachedOptions: FormattingOptions?

  public mutating func formattedDate(
    at index: UInt,
    options: FormattingOptions = FormattingOptions()
  ) -> String? {
    guard let timestamp = self[index] else { return nil }

    guard case .timestamp(let timeUnit, let timezone) = self.arrowData.type
    else {
      return options.fallbackToRaw ? "\(timestamp)" : nil
    }

    let date = dateFromTimestamp(timestamp, unit: timeUnit)

    if cachedFormatter == nil || cachedOptions != options {
      let formatter = DateFormatter()
      formatter.dateFormat = options.dateFormat
      formatter.locale = options.locale
      if options.includeTimezone, let timezone {
        formatter.timeZone = TimeZone(identifier: timezone)
      }
      cachedFormatter = formatter
      cachedOptions = options
    }
    return cachedFormatter?.string(from: date)
  }

  private func dateFromTimestamp(
    _ timestamp: Int64,
    unit: TimeUnit
  ) -> Date {
    let timeInterval: TimeInterval
    switch unit {
    case .second:
      timeInterval = TimeInterval(timestamp)
    case .millisecond:
      timeInterval = TimeInterval(timestamp) / 1_000
    case .microsecond:
      timeInterval = TimeInterval(timestamp) / 1_000_000
    case .nanosecond:
      timeInterval = TimeInterval(timestamp) / 1_000_000_000
    }
    return Date(timeIntervalSince1970: timeInterval)
  }

  // TODO: Mutating function to hack around cached formatter
  public mutating func asString(_ index: UInt) -> String {
    if let formatted = formattedDate(at: index) {
      return formatted
    } else {
      return "\(self[index] ?? 0)"
    }
  }
}

public struct BinaryArray: ArrowArray {
  public typealias ItemType = Data
  public let arrowData: ArrowData

  public init(_ arrowData: ArrowData) {
    self.arrowData = arrowData
  }

  public struct Options {
    public var printAsHex = false
    public var printEncoding: String.Encoding = .utf8
  }

  public var options = Options()

  public subscript(_ index: UInt) -> Data? {
    let offsetIndex = MemoryLayout<Int32>.stride * Int(index)
    if self.arrowData.isNull(index) {
      return nil
    }
    let offsets = self.arrowData.buffers[1]
    let values = self.arrowData.buffers[2]
    var startIndex: Int32 = 0
    if index > 0 {
      startIndex = offsets.rawPointer.advanced(by: offsetIndex)
        .load(as: Int32.self)
    }
    let endIndex = offsets.rawPointer.advanced(
      by: offsetIndex + MemoryLayout<Int32>.stride
    )
    .load(as: Int32.self)
    let arrayLength = Int(endIndex - startIndex)
    let rawPointer = values.rawPointer.advanced(by: Int(startIndex))
      .bindMemory(to: UInt8.self, capacity: arrayLength)
    let buffer = UnsafeBufferPointer<UInt8>(
      start: rawPointer, count: arrayLength)
    let byteArray = Array(buffer)
    return Data(byteArray)
  }

  public func asString(_ index: UInt) -> String {
    guard let data = self[index] else { return "" }
    if options.printAsHex {
      return data.hexEncodedString()
    } else {
      if let string = String(data: data, encoding: options.printEncoding) {
        return string
      } else {
        return "<unprintable>"
      }
    }
  }
}

public struct NestedArray: ArrowArray, AnyArrowArray {
  public typealias ItemType = [Any?]
  public let arrowData: ArrowData
  private var children: [AnyArrowArray]?

  public init(_ arrowData: ArrowData) throws(ArrowError) {
    self.arrowData = arrowData

    switch arrowData.type {
    case .list(let field):
      guard arrowData.children.count == 1 else {
        throw ArrowError.invalid("List array must have exactly one child")
      }
      self.children = [
        try ArrowArrayLoader.loadArray(
          field.type,
          with: arrowData.children[0]
        )
      ]
    case .strct(let _):
      var fields: [AnyArrowArray] = []
      for child in arrowData.children {
        fields.append(
          try ArrowArrayLoader.loadArray(child.type, with: child)
        )
      }
      self.children = fields
    default:
      throw .invalid(
        "NestedArray only supports list and struct types, got: \(arrowData.type)"
      )
    }
  }

  public subscript(_ index: UInt) -> [Any?]? {
    if self.arrowData.isNull(index) {
      return nil
    }
    guard let children = self.children else {
      return nil
    }
    switch arrowData.type {
    case .list(let _):
      guard let values = children.first else { return nil }
      let offsets = self.arrowData.buffers[1]
      let offsetIndex = Int(index) * MemoryLayout<Int32>.stride
      let startOffset = offsets.rawPointer.advanced(by: offsetIndex)
        .load(as: Int32.self)
      let endOffset = offsets.rawPointer.advanced(
        by: offsetIndex + MemoryLayout<Int32>.stride
      )
      .load(as: Int32.self)
      var items: [Any?] = []
      for i in startOffset..<endOffset {
        items.append(values.asAny(UInt(i)))
      }
      return items
    case .strct(let _):
      var result: [Any?] = []
      for field in children {
        result.append(field.asAny(index))
      }
      return result
    default:
      return nil
    }
  }

  public func asString(_ index: UInt) -> String {
    switch arrowData.type {
    case .list(let _):
      if self.arrowData.isNull(index) {
        return "null"
      }
      guard let list = self[index] else {
        return "null"
      }
      var output = "["
      for (i, item) in list.enumerated() {
        if i > 0 {
          output.append(",")
        }
        switch item {
        case nil:
          output.append("null")
        case let asStringItem as AnyArrowArray:
          output.append(asStringItem.asString(0))
        case let someItem?:
          output.append("\(someItem)")
        }
      }
      output.append("]")
      return output
    case .strct(let _):
      if self.arrowData.isNull(index) {
        return ""
      }
      var output = "{"
      if let children = self.children {
        let parts = children.compactMap { child in
          child.asString(index)
        }
        output.append(parts.joined(separator: ","))
      }
      output += "}"
      return output
    default:
      return ""
    }
  }

  public var fields: [AnyArrowArray]? {
    if case .strct(_) = arrowData.type {
      return children
    } else {
      return nil
    }
  }

  public var values: AnyArrowArray? {
    if case .list(_) = arrowData.type {
      return children?.first
    } else {
      return nil
    }
  }
}
