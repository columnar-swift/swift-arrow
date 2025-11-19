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

public class ArrowDecoder: Decoder {
  var rbIndex: UInt = 0
  var singleRBCol: Int = 0
  public var codingPath: [CodingKey] = []
  public var userInfo: [CodingUserInfoKey: Any] = [:]
  public let rb: RecordBatchX
  public let nameToCol: [String: AnyArrowArray]
  public let columns: [AnyArrowArray]

  public init(_ decoder: ArrowDecoder) {
    self.userInfo = decoder.userInfo
    self.codingPath = decoder.codingPath
    self.rb = decoder.rb
    self.columns = decoder.columns
    self.nameToCol = decoder.nameToCol
    self.rbIndex = decoder.rbIndex
  }

  public init(_ rb: RecordBatchX) {
    self.rb = rb
    var colMapping: [String: AnyArrowArray] = [:]
    var columns: [AnyArrowArray] = []
    for index in 0..<self.rb.schema.fields.count {
      let field = self.rb.schema.fields[index]
      columns.append(self.rb.column(index))
      colMapping[field.name] = self.rb.column(index)
    }

    self.columns = columns
    self.nameToCol = colMapping
  }

  public func decode<T: Decodable, U: Decodable>(
    _ type: [T: U].Type
  ) throws -> [T: U] {
    var output: [T: U] = [:]
    if rb.columnCount != 2 {
      throw ArrowError.invalid(
        "RecordBatch column count of 2 is required to decode to map"
      )
    }
    for index in 0..<rb.length {
      self.rbIndex = index
      self.singleRBCol = 0
      let key = try T.init(from: self)
      self.singleRBCol = 1
      let value = try U.init(from: self)
      output[key] = value
    }
    self.singleRBCol = 0
    return output
  }

  public func decode<T: Decodable>(_ type: T.Type) throws -> [T] {
    var output: [T] = []
    for index in 0..<rb.length {
      self.rbIndex = index
      output.append(try type.init(from: self))
    }
    return output
  }

  public func container<Key>(
    keyedBy type: Key.Type
  ) -> KeyedDecodingContainer<Key> where Key: CodingKey {
    let container = ArrowKeyedDecoding<Key>(self, codingPath: codingPath)
    return KeyedDecodingContainer(container)
  }

  public func unkeyedContainer() -> UnkeyedDecodingContainer {
    ArrowUnkeyedDecoding(self, codingPath: codingPath)
  }

  public func singleValueContainer() -> SingleValueDecodingContainer {
    ArrowSingleValueDecoding(self, codingPath: codingPath)
  }

  func getCol(_ name: String) throws -> AnyArrowArray {
    guard let col = self.nameToCol[name] else {
      throw ArrowError.invalid("Column for key \"\(name)\" not found")
    }

    return col
  }

  func getCol(_ index: Int) throws -> AnyArrowArray {
    if index >= self.columns.count {
      throw ArrowError.outOfBounds(index: Int64(index))
    }

    return self.columns[index]
  }

  func doDecode<T>(_ key: CodingKey) throws -> T? {
    let array: AnyArrowArray = try self.getCol(key.stringValue)
    return array.asAny(self.rbIndex) as? T
  }

  func doDecode<T>(_ col: Int) throws -> T? {
    let array: AnyArrowArray = try self.getCol(col)
    return array.asAny(self.rbIndex) as? T
  }

  func isNull(_ key: CodingKey) throws -> Bool {
    let array: AnyArrowArray = try self.getCol(key.stringValue)
    return array.asAny(self.rbIndex) == nil
  }

  func isNull(_ col: Int) throws -> Bool {
    let array: AnyArrowArray = try self.getCol(col)
    return array.asAny(self.rbIndex) == nil
  }
}

private struct ArrowUnkeyedDecoding: UnkeyedDecodingContainer {
  var codingPath: [CodingKey]
  var count: Int? = 0
  var isAtEnd: Bool = false
  var currentIndex: Int = 0
  let decoder: ArrowDecoder

  init(_ decoder: ArrowDecoder, codingPath: [CodingKey]) {
    self.decoder = decoder
    self.codingPath = codingPath
    self.count = self.decoder.columns.count
  }

  mutating func increment() {
    self.currentIndex += 1
    self.isAtEnd = self.currentIndex >= self.count ?? 0
  }

  mutating func decodeNil() throws -> Bool {
    defer { increment() }
    return try self.decoder.isNull(self.currentIndex)
  }

  mutating func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
    if type == Int8?.self || type == Int16?.self || type == Int32?.self
      || type == Int64?.self
      || type == UInt8?.self || type == UInt16?.self || type == UInt32?.self
      || type == UInt64?.self
      || type == String?.self || type == Double?.self || type == Float?.self
      || type == Date?.self
      || type == Bool?.self || type == Bool.self || type == Int8.self
      || type == Int16.self
      || type == Int32.self || type == Int64.self || type == UInt8.self
      || type == UInt16.self
      || type == UInt32.self || type == UInt64.self || type == String.self
      || type == Double.self
      || type == Float.self || type == Date.self
    {
      defer { increment() }
      guard let value: T = try self.decoder.doDecode(self.currentIndex) else {
        throw ArrowError.invalid("Failed to decode value for \(type)")
      }
      return value
    } else {
      throw ArrowError.invalid("Type \(type) is currently not supported")
    }
  }

  func nestedContainer<NestedKey>(
    keyedBy type: NestedKey.Type
  ) throws -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
    throw ArrowError.invalid("Nested decoding is currently not supported.")
  }

  func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
    throw ArrowError.invalid("Nested decoding is currently not supported.")
  }

  func superDecoder() throws -> Decoder {
    throw ArrowError.invalid("super decoding is currently not supported.")
  }
}

private struct ArrowKeyedDecoding<Key: CodingKey>:
  KeyedDecodingContainerProtocol
{
  var codingPath: [CodingKey] = []
  var allKeys: [Key] = []
  let decoder: ArrowDecoder

  init(_ decoder: ArrowDecoder, codingPath: [CodingKey]) {
    self.decoder = decoder
    self.codingPath = codingPath
  }

  func contains(_ key: Key) -> Bool {
    self.decoder.nameToCol.keys.contains(key.stringValue)
  }

  func decodeNil(forKey key: Key) throws -> Bool {
    try self.decoder.isNull(key)
  }

  func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool {
    guard let value: Bool = try self.decoder.doDecode(key) else {
      throw ArrowError.invalid("Failed to decode \(type) for key \(key)")
    }
    return value
  }

  func decode(_ type: String.Type, forKey key: Key) throws -> String {
    guard let value: String = try self.decoder.doDecode(key) else {
      throw ArrowError.invalid("Failed to decode \(type) for key \(key)")
    }
    return value
  }

  func decode(_ type: Double.Type, forKey key: Key) throws -> Double {
    guard let value: Double = try self.decoder.doDecode(key) else {
      throw ArrowError.invalid("Failed to decode \(type) for key \(key)")
    }
    return value
  }

  func decode(_ type: Float.Type, forKey key: Key) throws -> Float {
    guard let value: Float = try self.decoder.doDecode(key) else {
      throw ArrowError.invalid("Failed to decode \(type) for key \(key)")
    }
    return value
  }

  func decode(_ type: Int.Type, forKey key: Key) throws -> Int {
    throw ArrowError.invalid(
      "Int type is not supported (please use Int8, Int16, Int32 or Int64)")
  }

  func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 {
    guard let value: Int8 = try self.decoder.doDecode(key) else {
      throw ArrowError.invalid("Failed to decode \(type) for key \(key)")
    }
    return value
  }

  func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 {
    guard let value: Int16 = try self.decoder.doDecode(key) else {
      throw ArrowError.invalid("Failed to decode \(type) for key \(key)")
    }
    return value
  }

  func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 {
    guard let value: Int32 = try self.decoder.doDecode(key) else {
      throw ArrowError.invalid("Failed to decode \(type) for key \(key)")
    }
    return value
  }

  func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 {
    guard let value: Int64 = try self.decoder.doDecode(key) else {
      throw ArrowError.invalid("Failed to decode \(type) for key \(key)")
    }
    return value
  }

  func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt {
    throw ArrowError.invalid(
      "UInt type is not supported (please use UInt8, UInt16, UInt32 or UInt64)")
  }

  func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 {
    guard let value: UInt8 = try self.decoder.doDecode(key) else {
      throw ArrowError.invalid("Failed to decode \(type) for key \(key)")
    }
    return value
  }

  func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 {
    guard let value: UInt16 = try self.decoder.doDecode(key) else {
      throw ArrowError.invalid("Failed to decode \(type) for key \(key)")
    }
    return value
  }

  func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 {
    guard let value: UInt32 = try self.decoder.doDecode(key) else {
      throw ArrowError.invalid("Failed to decode \(type) for key \(key)")
    }
    return value
  }

  func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 {
    guard let value: UInt64 = try self.decoder.doDecode(key) else {
      throw ArrowError.invalid("Failed to decode \(type) for key \(key)")
    }
    return value
  }

  func decode<T>(_ type: T.Type, forKey key: Key) throws -> T
  where T: Decodable {
    if ArrowArrayBuilders.isValidBuilderType(type) || type == Date.self {
      guard let value: T = try self.decoder.doDecode(key) else {
        throw ArrowError.invalid("Failed to decode \(type) for key \(key)")
      }
      return value
    } else {
      throw ArrowError.invalid("Type \(type) is currently not supported")
    }
  }

  func nestedContainer<NestedKey>(
    keyedBy type: NestedKey.Type,
    forKey key: Key
  ) throws -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
    throw ArrowError.invalid("Nested decoding is currently not supported.")
  }

  func nestedUnkeyedContainer(forKey key: Key) throws
    -> UnkeyedDecodingContainer
  {
    throw ArrowError.invalid("Nested decoding is currently not supported.")
  }

  func superDecoder() throws -> Decoder {
    throw ArrowError.invalid("super decoding is currently not supported.")
  }

  func superDecoder(forKey key: Key) throws -> Decoder {
    throw ArrowError.invalid("super decoding is currently not supported.")
  }
}

private struct ArrowSingleValueDecoding: SingleValueDecodingContainer {
  var codingPath: [CodingKey] = []
  let decoder: ArrowDecoder

  init(_ decoder: ArrowDecoder, codingPath: [CodingKey]) {
    self.decoder = decoder
    self.codingPath = codingPath
  }

  func decodeNil() -> Bool {
    do {
      return try self.decoder.isNull(self.decoder.singleRBCol)
    } catch {
      return false
    }
  }

  func decode(_ type: Bool.Type) throws -> Bool {
    guard let value: Bool = try self.decoder.doDecode(self.decoder.singleRBCol)
    else {
      throw ArrowError.invalid("Failed to decode \(type)")
    }
    return value
  }

  func decode(_ type: String.Type) throws -> String {
    guard
      let value: String = try self.decoder.doDecode(self.decoder.singleRBCol)
    else {
      throw ArrowError.invalid("Failed to decode \(type)")
    }
    return value
  }

  func decode(_ type: Double.Type) throws -> Double {
    guard
      let value: Double = try self.decoder.doDecode(self.decoder.singleRBCol)
    else {
      throw ArrowError.invalid("Failed to decode \(type)")
    }
    return value
  }

  func decode(_ type: Float.Type) throws -> Float {
    guard let value: Float = try self.decoder.doDecode(self.decoder.singleRBCol)
    else {
      throw ArrowError.invalid("Failed to decode \(type)")
    }
    return value
  }

  func decode(_ type: Int.Type) throws -> Int {
    throw ArrowError.invalid(
      "Int type is not supported (please use Int8, Int16, Int32 or Int64)")
  }

  func decode(_ type: Int8.Type) throws -> Int8 {
    guard let value: Int8 = try self.decoder.doDecode(self.decoder.singleRBCol)
    else {
      throw ArrowError.invalid("Failed to decode \(type)")
    }
    return value
  }

  func decode(_ type: Int16.Type) throws -> Int16 {
    guard let value: Int16 = try self.decoder.doDecode(self.decoder.singleRBCol)
    else {
      throw ArrowError.invalid("Failed to decode \(type)")
    }
    return value
  }

  func decode(_ type: Int32.Type) throws -> Int32 {
    guard let value: Int32 = try self.decoder.doDecode(self.decoder.singleRBCol)
    else {
      throw ArrowError.invalid("Failed to decode \(type)")
    }
    return value
  }

  func decode(_ type: Int64.Type) throws -> Int64 {
    guard let value: Int64 = try self.decoder.doDecode(self.decoder.singleRBCol)
    else {
      throw ArrowError.invalid("Failed to decode \(type)")
    }
    return value
  }

  func decode(_ type: UInt.Type) throws -> UInt {
    throw ArrowError.invalid(
      "UInt type is not supported (please use UInt8, UInt16, UInt32 or UInt64)")
  }

  func decode(_ type: UInt8.Type) throws -> UInt8 {
    guard let value: UInt8 = try self.decoder.doDecode(self.decoder.singleRBCol)
    else {
      throw ArrowError.invalid("Failed to decode \(type)")
    }
    return value
  }

  func decode(_ type: UInt16.Type) throws -> UInt16 {
    guard
      let value: UInt16 = try self.decoder.doDecode(self.decoder.singleRBCol)
    else {
      throw ArrowError.invalid("Failed to decode \(type)")
    }
    return value
  }

  func decode(_ type: UInt32.Type) throws -> UInt32 {
    guard
      let value: UInt32 = try self.decoder.doDecode(self.decoder.singleRBCol)
    else {
      throw ArrowError.invalid("Failed to decode \(type)")
    }
    return value
  }

  func decode(_ type: UInt64.Type) throws -> UInt64 {
    guard
      let value: UInt64 = try self.decoder.doDecode(self.decoder.singleRBCol)
    else {
      throw ArrowError.invalid("Failed to decode \(type)")
    }
    return value
  }

  func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
    if ArrowArrayBuilders.isValidBuilderType(type) || type == Date.self {
      guard let value: T = try self.decoder.doDecode(self.decoder.singleRBCol)
      else {
        throw ArrowError.invalid("Failed to decode \(type)")
      }
      return value
    } else {
      throw ArrowError.invalid("Type \(type) is currently not supported")
    }
  }
}
