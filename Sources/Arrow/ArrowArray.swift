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

public protocol ArrowArrayHolder {
  var type: ArrowType { get }
  var length: UInt { get }
  var nullCount: UInt { get }
  var array: AnyArray { get }
  var data: ArrowData { get }
  var getBufferData: () -> [Data] { get }
  var getBufferDataSizes: () -> [Int] { get }
}

public struct ArrowArrayHolderImpl: ArrowArrayHolder {
  public let data: ArrowData
  public let type: ArrowType
  public let length: UInt
  public let nullCount: UInt
  public let array: AnyArray
  public let getBufferData: () -> [Data]
  public let getBufferDataSizes: () -> [Int]
  public init<T>(_ arrowArray: ArrowArray<T>) {
    self.array = arrowArray
    self.data = arrowArray.arrowData
    self.length = arrowArray.length
    self.type = arrowArray.arrowData.type
    self.nullCount = arrowArray.nullCount
    self.getBufferData = { () -> [Data] in
      var bufferData: [Data] = []
      for buffer in arrowArray.arrowData.buffers {
        bufferData.append(Data())
        buffer.append(to: &bufferData[bufferData.count - 1])
      }
      return bufferData
    }

    self.getBufferDataSizes = { () -> [Int] in
      var bufferDataSizes: [Int] = []
      for buffer in arrowArray.arrowData.buffers {
        bufferDataSizes.append(Int(buffer.capacity))
      }
      return bufferDataSizes
    }
  }

  public static func loadArray(
    _ arrowType: ArrowType,
    with arrowData: ArrowData
  ) throws(ArrowError) -> ArrowArrayHolder {
    switch arrowType {
    case .int8:
      return try ArrowArrayHolderImpl(FixedArray<Int8>(arrowData))
    case .int16:
      return try ArrowArrayHolderImpl(FixedArray<Int16>(arrowData))
    case .int32:
      return try ArrowArrayHolderImpl(FixedArray<Int32>(arrowData))
    case .int64:
      return try ArrowArrayHolderImpl(FixedArray<Int64>(arrowData))
    case .uint8:
      return try ArrowArrayHolderImpl(FixedArray<UInt8>(arrowData))
    case .uint16:
      return try ArrowArrayHolderImpl(FixedArray<UInt16>(arrowData))
    case .uint32:
      return try ArrowArrayHolderImpl(FixedArray<UInt32>(arrowData))
    case .uint64:
      return try ArrowArrayHolderImpl(FixedArray<UInt64>(arrowData))
    case .float64:
      return try ArrowArrayHolderImpl(FixedArray<Double>(arrowData))
    case .float32:
      return try ArrowArrayHolderImpl(FixedArray<Float>(arrowData))
    case .date32:
      return try ArrowArrayHolderImpl(Date32Array(arrowData))
    case .date64:
      return try ArrowArrayHolderImpl(Date64Array(arrowData))
    case .time32:
      return try ArrowArrayHolderImpl(Time32Array(arrowData))
    case .time64:
      return try ArrowArrayHolderImpl(Time64Array(arrowData))
    case .timestamp:
      return try ArrowArrayHolderImpl(TimestampArray(arrowData))
    case .utf8:
      return try ArrowArrayHolderImpl(StringArray(arrowData))
    case .boolean:
      return try ArrowArrayHolderImpl(BoolArray(arrowData))
    case .binary:
      return try ArrowArrayHolderImpl(BinaryArray(arrowData))
    case .strct(let _):
      return try ArrowArrayHolderImpl(NestedArray(arrowData))
    case .list(let _):
      return try ArrowArrayHolderImpl(NestedArray(arrowData))
    default:
      throw ArrowError.invalid("Array not found for type: \(arrowType)")
    }
  }
}

public class ArrowArray<T>: AsString, AnyArray {
  //  public typealias ItemType = T
  public let arrowData: ArrowData
  public var nullCount: UInt { self.arrowData.nullCount }
  public var length: UInt { self.arrowData.length }

  public required init(_ arrowData: ArrowData) throws(ArrowError) {
    self.arrowData = arrowData
  }

  public func isNull(at index: UInt) throws -> Bool {
    if index >= self.length {
      throw ArrowError.outOfBounds(index: Int64(index))
    }
    return self.arrowData.isNull(index)
  }

  public subscript(_ index: UInt) -> T? {
    fatalError("subscript() has not been implemented")
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

public class FixedArray<T>: ArrowArray<T> {
  public override subscript(_ index: UInt) -> T? {
    if self.arrowData.isNull(index) {
      return nil
    }

    let byteOffset = self.arrowData.stride * Int(index)
    return self.arrowData.buffers[1].rawPointer.advanced(by: byteOffset).load(
      as: T.self)
  }
}

public class StringArray: ArrowArray<String> {
  public override subscript(_ index: UInt) -> String? {
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

public class BoolArray: ArrowArray<Bool> {
  public override subscript(_ index: UInt) -> Bool? {
    if self.arrowData.isNull(index) {
      return nil
    }
    let valueBuffer = self.arrowData.buffers[1]
    return BitUtility.isSet(index, buffer: valueBuffer)
  }
}

public class Date32Array: ArrowArray<Date> {
  public override subscript(_ index: UInt) -> Date? {
    if self.arrowData.isNull(index) {
      return nil
    }
    let byteOffset = self.arrowData.stride * Int(index)
    let milliseconds = self.arrowData.buffers[1].rawPointer.advanced(
      by: byteOffset
    ).load(
      as: UInt32.self)
    return Date(timeIntervalSince1970: TimeInterval(milliseconds * 86400))
  }
}

public class Date64Array: ArrowArray<Date> {
  public override subscript(_ index: UInt) -> Date? {
    if self.arrowData.isNull(index) {
      return nil
    }
    let byteOffset = self.arrowData.stride * Int(index)
    let milliseconds = self.arrowData.buffers[1].rawPointer.advanced(
      by: byteOffset
    ).load(
      as: UInt64.self)
    return Date(timeIntervalSince1970: TimeInterval(milliseconds / 1000))
  }
}

public class Time32Array: FixedArray<Time32> {}
public class Time64Array: FixedArray<Time64> {}

public class TimestampArray: FixedArray<Timestamp> {

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

  public func formattedDate(
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

  public override func asString(_ index: UInt) -> String {
    if let formatted = formattedDate(at: index) {
      return formatted
    }
    return super.asString(index)
  }
}

public class BinaryArray: ArrowArray<Data> {

  public struct Options {
    public var printAsHex = false
    public var printEncoding: String.Encoding = .utf8
  }

  public var options = Options()

  public override subscript(_ index: UInt) -> Data? {
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

  public override func asString(_ index: UInt) -> String {
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

public class NestedArray: ArrowArray<[Any?]> {
  private var children: [ArrowArrayHolder]?

  public required init(_ arrowData: ArrowData) throws(ArrowError) {
    try super.init(arrowData)
    switch arrowData.type {
    case .list(let field):
      guard arrowData.children.count == 1 else {
        throw ArrowError.invalid("List array must have exactly one child")
      }
      self.children = [
        try ArrowArrayHolderImpl.loadArray(
          field.type,
          with: arrowData.children[0]
        )
      ]
    case .strct(let _):
      var fields: [ArrowArrayHolder] = []
      for child in arrowData.children {
        fields.append(
          try ArrowArrayHolderImpl.loadArray(child.type, with: child)
        )
      }
      self.children = fields
    default:
      throw .invalid(
        "NestedArray only supports list and struct types, got: \(arrowData.type)"
      )
    }
  }

  public override subscript(_ index: UInt) -> [Any?]? {
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
        items.append(values.array.asAny(UInt(i)))
      }
      return items
    case .strct(let _):
      var result: [Any?] = []
      for field in children {
        result.append(field.array.asAny(index))
      }
      return result
    default:
      return nil
    }
  }

  public override func asString(_ index: UInt) -> String {
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
        case let asStringItem as AsString:
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
          (child.array as? AsString)?.asString(index)
        }
        output.append(parts.joined(separator: ","))
      }
      output += "}"
      return output
    default:
      return ""
    }
  }

  public var fields: [ArrowArrayHolder]? {
    if case .strct(_) = arrowData.type {
      return children
    } else {
      return nil
    }
  }

  public var values: ArrowArrayHolder? {
    if case .list(_) = arrowData.type {
      return children?.first
    } else {
      return nil
    }
  }
}
