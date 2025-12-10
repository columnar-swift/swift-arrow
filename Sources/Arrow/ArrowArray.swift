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

import ArrowC
import Foundation

/// A type-erased ArrowArray.
public protocol AnyArrowArray {
  var type: ArrowType { get }
  var length: UInt { get }
  var nullCount: UInt { get }
  var arrowData: ArrowData { get }
  var bufferData: [Data] { get }  // TODO: remove
  var bufferDataSizes: [Int] { get }  // TODO: remove
  func asAny(_ index: UInt) -> Any?
  func asString(_ index: UInt) -> String
  func setCArrayPtr(_ cArrayPtr: UnsafePointer<ArrowC.ArrowArray>?)
}

/// The interface for Arrow array types.
public protocol ArrowArray<ItemType>: AnyArrowArray {
  associatedtype ItemType
  init(_ arrowData: ArrowData) throws(ArrowError)
  subscript(_ index: UInt) -> ItemType? { get }
}

public class ArrowArrayBase<T>: ArrowArray {

  public var arrowData: ArrowData
  public var cArrayPtr: UnsafePointer<ArrowC.ArrowArray>? = nil

  required public init(_ arrowData: ArrowData) throws(ArrowError) {
    self.arrowData = arrowData
  }

  public subscript(_ index: UInt) -> T? {
    fatalError("Base class is abstract.")
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

  public func setCArrayPtr(_ cArrayPtr: UnsafePointer<ArrowC.ArrowArray>?) {
    self.cArrayPtr = cArrayPtr
  }

  deinit {
    if let cArrayPtr {
      ArrowCImporter.release(cArrayPtr)
    }
  }
}

extension ArrowArrayBase {
  public var nullCount: UInt {
    arrowData.nullCount
  }

  public var length: UInt {
    arrowData.length
  }

  public var type: ArrowType {
    arrowData.type
  }

  // TODO: Remove
  public var bufferData: [Data] {
    arrowData.bufferData
  }

  // TODO: Remove
  public var bufferDataSizes: [Int] {
    arrowData.bufferDataSizes
  }

  public func isNull(at index: UInt) throws(ArrowError) -> Bool {
    if index >= self.length {
      throw .init(.outOfBounds(index: Int64(index)))
    }
    return arrowData.isNull(index)
  }
}

// MARK: Fixed Arrays

public class FixedArray<T>: ArrowArrayBase<T> where T: BitwiseCopyable {

  public override subscript(_ index: UInt) -> ItemType? {
    if arrowData.isNull(index) {
      return nil
    }
    let value: ItemType = arrowData.load(at: index)
    return value
  }
}

public class StringArray: ArrowArrayBase<String> {

  public override subscript(_ index: UInt) -> String? {
    if self.arrowData.isNull(index) {
      return nil
    }
    let offsetBuffer: OffsetsBuffer = arrowData.offsets
    let (startIndex, endIndex) = offsetBuffer.offsets(at: Int(index))
    let arrayLength = Int(endIndex - startIndex)
    let value: String = self.arrowData.loadVariable(
      at: Int(startIndex), arrayLength: arrayLength)
    return value
  }
}

public class BoolArray: ArrowArrayBase<Bool> {

  public override subscript(_ index: UInt) -> Bool? {
    if self.arrowData.isNull(index) {
      return nil
    }
    return arrowData.isNullValue(at: index)
  }
}

public class Date32Array: ArrowArrayBase<Date> {

  public override subscript(_ index: UInt) -> Date? {
    if self.arrowData.isNull(index) {
      return nil
    }
    let milliseconds: UInt32 = arrowData.load(at: index)
    return Date(timeIntervalSince1970: TimeInterval(milliseconds * 86400))
  }
}

public class Date64Array: ArrowArrayBase<Date> {

  public override subscript(_ index: UInt) -> Date? {
    if self.arrowData.isNull(index) {
      return nil
    }

    let milliseconds: UInt64 = self.arrowData.load(at: index)
    return Date(timeIntervalSince1970: TimeInterval(milliseconds / 1000))
  }
}

public typealias Time64Array = FixedArray<Time64>

public typealias Time32Array = FixedArray<Time32>

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
    } else {
      return "\(self[index] ?? 0)"
    }
  }
}

public class BinaryArray: ArrowArrayBase<Data> {

  public struct Options {
    public var printAsHex = false
    public var printEncoding: String.Encoding = .utf8
  }

  public var options = Options()

  public override subscript(_ index: UInt) -> Data? {
    if self.arrowData.isNull(index) {
      return nil
    }

    let (startIndex, endIndex) = arrowData.offsets.offsets(at: Int(index))

    let arrayLength = Int(endIndex - startIndex)

    let data: Data = self.arrowData.loadVariable(
      at: Int(startIndex), arrayLength: arrayLength)
    return data
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

public class NestedArray: ArrowArrayBase<[Any?]> {

  private var children: [AnyArrowArray]?

  public required init(
    _ arrowData: ArrowData
  ) throws(ArrowError) {
    try super.init(arrowData)

    switch arrowData.type {
    case .list(let field):
      guard arrowData.children.count == 1 else {
        throw ArrowError(.invalid("List array must have exactly one child"))
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
      throw .init(
        .invalid(
          "NestedArray only supports list and struct types, got: \(arrowData.type)"
        ))
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

      let (startIndex, endIndex) = arrowData.offsets.offsets(at: Int(index))
      var items: [Any?] = []
      for i in startIndex..<endIndex {
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
