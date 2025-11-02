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

// MARK: Array builder interface.

/// A type which builds a type-erased `ArrowArray`.
public protocol AnyArrowArrayBuilder {
  /// Returns an unparameterised `ArrowArray`.
  /// - Returns: The type-erased Arrow array.
  func toAnyArrowArray() throws(ArrowError) -> AnyArrowArray
  func appendAny(_ val: Any?)
}

/// A type which can build an `ArrowArray`of `ItemType`.
public protocol ArrowArrayBuilder {
  associatedtype BufferBuilder: ArrowBufferBuilder
  associatedtype ArrayType: ArrowArray
  where ArrayType.ItemType == BufferBuilder.ItemType

  func append(_ vals: BufferBuilder.ItemType?...)
  func append(_ vals: [BufferBuilder.ItemType?])
  func append(_ val: BufferBuilder.ItemType?)
  func appendAny(_ val: Any?)
  func finish() throws(ArrowError) -> ArrayType
}

internal protocol ArrowArrayBuilderInternal: ArrowArrayBuilder {
  var arrowType: ArrowType { get }
  var bufferBuilder: BufferBuilder { get }
}

extension ArrowArrayBuilderInternal {

  var length: UInt { self.bufferBuilder.length }
  var capacity: UInt { self.bufferBuilder.capacity }
  var nullCount: UInt { self.bufferBuilder.nullCount }
  var offset: UInt { self.bufferBuilder.offset }

  public func append(_ vals: BufferBuilder.ItemType?...) {
    for val in vals {
      self.bufferBuilder.append(val)
    }
  }

  public func append(_ vals: [BufferBuilder.ItemType?]) {
    for val in vals {
      self.bufferBuilder.append(val)
    }
  }

  public func append(_ val: BufferBuilder.ItemType?) {
    self.bufferBuilder.append(val)
  }

  public func appendAny(_ val: Any?) {
    self.bufferBuilder.append(val as? BufferBuilder.ItemType)
  }

  /// Returns the byte width of this type if it is a primitive type.
  public func stride() -> Int {
    self.arrowType.getStride()
  }

  public func toAnyArrowArray() throws(ArrowError) -> AnyArrowArray {
    try self.finish()
  }
}

// MARK: Base implementation.

// Note: It would be preferable to move all of this to a protocol, however
// ListArrayBuilder overrides finish. This is delicate because protocol
// extension method dispatching means the
public class ArrowArrayBuilderBase<
  BufferBuilder: ArrowBufferBuilder,
  ArrayType: ArrowArray<BufferBuilder.ItemType>
>: AnyArrowArrayBuilder, ArrowArrayBuilderInternal {
  let arrowType: ArrowType
  let bufferBuilder: BufferBuilder

  fileprivate init(_ type: ArrowType) throws(ArrowError) {
    self.arrowType = type
    self.bufferBuilder = BufferBuilder()
  }

  public func finish() throws(ArrowError) -> ArrayType {
    let buffers = self.bufferBuilder.finish()
    let arrowData = ArrowData(
      self.arrowType,
      buffers: buffers,
      nullCount: self.nullCount
    )
    let array = try ArrayType(arrowData)
    return array
  }
}

/// An array builder for numeric types.
public class NumberArrayBuilder<ItemType>: ArrowArrayBuilderBase<
  FixedBufferBuilder<ItemType>,
  FixedArray<ItemType>
>
where ItemType: Numeric, ItemType: BitwiseCopyable {
  fileprivate convenience init() throws(ArrowError) {
    try self.init(try ArrowTypeConverter.infoForNumericType(ItemType.self))
  }
}

/// A `String` array builder.
public class StringArrayBuilder: ArrowArrayBuilderBase<
  VariableBufferBuilder<String>,
  StringArray
>
{
  fileprivate convenience init() throws(ArrowError) {
    try self.init(.utf8)
  }
}

/// A `Data` array builder.
public class BinaryArrayBuilder: ArrowArrayBuilderBase<
  VariableBufferBuilder<Data>,
  BinaryArray
>
{
  fileprivate convenience init() throws(ArrowError) {
    try self.init(.binary)
  }
}

/// A  `Bool` array builder.
public class BoolArrayBuilder: ArrowArrayBuilderBase<
  BoolBufferBuilder, BoolArray
>
{
  fileprivate convenience init() throws(ArrowError) {
    try self.init(.boolean)
  }
}

/// A 32-bit date array builder.
public class Date32ArrayBuilder: ArrowArrayBuilderBase<
  Date32BufferBuilder,
  Date32Array
>
{
  fileprivate convenience init() throws(ArrowError) {
    try self.init(.date32)
  }
}

/// A 64-bit date array builder.
public class Date64ArrayBuilder: ArrowArrayBuilderBase<
  Date64BufferBuilder,
  Date64Array
>
{
  fileprivate convenience init() throws(ArrowError) {
    try self.init(.date64)
  }
}

// A 32-bit elaspsed time builder.
public class Time32ArrayBuilder: ArrowArrayBuilderBase<
  FixedBufferBuilder<Time32>,
  Time32Array
>
{
  fileprivate convenience init(_ unit: TimeUnit) throws(ArrowError) {
    try self.init(.time32(unit))
  }
}

// A 64-bit elaspsed time builder.
public class Time64ArrayBuilder: ArrowArrayBuilderBase<
  FixedBufferBuilder<Time64>,
  Time64Array
>
{
  fileprivate convenience init(_ unit: TimeUnit) throws(ArrowError) {
    try self.init(.time64(unit))
  }
}

// A Timestamp array builder.
public class TimestampArrayBuilder: ArrowArrayBuilderBase<
  FixedBufferBuilder<Int64>,
  TimestampArray
>
{
  fileprivate convenience init(
    _ unit: TimeUnit, timezone: String? = nil
  ) throws(ArrowError) {
    try self.init(.timestamp(unit, timezone))
  }
}

// MARK: Struct array builder.

/// Builds an array of structs.
public class StructArrayBuilder: ArrowArrayBuilderBase<
  StructBufferBuilder,
  NestedArray
>
{
  let builders: [any AnyArrowArrayBuilder]
  let fields: [ArrowField]
  public init(
    _ fields: [ArrowField],
    builders: [any AnyArrowArrayBuilder]
  ) throws(ArrowError) {
    self.fields = fields
    self.builders = builders
    try super.init(.strct(fields))
    self.bufferBuilder.initializeTypeInfo(fields)
  }

  public init(_ fields: [ArrowField]) throws(ArrowError) {
    self.fields = fields
    var builders: [any AnyArrowArrayBuilder] = []
    for field in fields {
      builders.append(
        try ArrowArrayBuilders.loadBuilder(arrowType: field.type))
    }
    self.builders = builders
    try super.init(.strct(fields))
  }

  public func append(_ values: [Any?]?) {
    self.bufferBuilder.append(values)
    if let anyValues = values {
      for index in 0..<builders.count {
        self.builders[index].appendAny(anyValues[index])
      }
    } else {
      for index in 0..<builders.count {
        self.builders[index].appendAny(nil)
      }
    }
  }

  public override func finish() throws(ArrowError) -> ArrayType {
    let buffers = self.bufferBuilder.finish()
    var childData: [ArrowData] = []
    for builder in self.builders {
      childData.append(try builder.toAnyArrowArray().arrowData)
    }
    let arrowData = ArrowData(
      self.arrowType, buffers: buffers,
      children: childData,
      nullCount: self.nullCount,
      length: self.length)
    let structArray = try NestedArray(arrowData)
    return structArray
  }
}

// MARK: List array builder.

/// Builds a `NestedArray`containing lists of `ItemType`.
///
/// Both lists and items in lists are nullablie.
public class ListArrayBuilder: ArrowArrayBuilderBase<
  ListBufferBuilder,
  NestedArray
>
{
  let valueBuilder: any AnyArrowArrayBuilder

  public override init(_ elementType: ArrowType) throws(ArrowError) {
    guard case .list(let field) = elementType else {
      throw .invalid("Expected a field with type .list")
    }
    self.valueBuilder = try ArrowArrayBuilders.loadBuilder(
      arrowType: field.type
    )
    try super.init(elementType)
  }

  // Overrides the protocol extension.
  // Swift currently provides no marker for this.
  public func append(_ values: [Any?]?) {
    self.bufferBuilder.append(values)
    if let vals = values {
      for val in vals {
        self.valueBuilder.appendAny(val)
      }
    }
  }

  public override func finish() throws(ArrowError) -> ArrayType {
    let buffers = self.bufferBuilder.finish()
    let childData = try valueBuilder.toAnyArrowArray().arrowData
    let arrowData = ArrowData(
      self.arrowType,
      buffers: buffers,
      children: [childData],
      nullCount: self.nullCount,
      length: self.length
    )
    return try NestedArray(arrowData)
  }
}

public enum ArrowArrayBuilders {
  public static func builder(
    for builderType: Any.Type
  ) throws(ArrowError) -> AnyArrowArrayBuilder {
    if builderType == Int8.self || builderType == Int8?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder()
        as NumberArrayBuilder<Int8>
    } else if builderType == Int16.self || builderType == Int16?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder()
        as NumberArrayBuilder<Int16>
    } else if builderType == Int32.self || builderType == Int32?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder()
        as NumberArrayBuilder<Int32>
    } else if builderType == Int64.self || builderType == Int64?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder()
        as NumberArrayBuilder<Int64>
    } else if builderType == Float.self || builderType == Float?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder()
        as NumberArrayBuilder<Float>
    } else if builderType == UInt8.self || builderType == UInt8?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder()
        as NumberArrayBuilder<UInt8>
    } else if builderType == UInt16.self || builderType == UInt16?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder()
        as NumberArrayBuilder<UInt16>
    } else if builderType == UInt32.self || builderType == UInt32?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder()
        as NumberArrayBuilder<UInt32>
    } else if builderType == UInt64.self || builderType == UInt64?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder()
        as NumberArrayBuilder<UInt64>
    } else if builderType == Double.self || builderType == Double?.self {
      return try ArrowArrayBuilders.loadNumberArrayBuilder()
        as NumberArrayBuilder<Double>
    } else if builderType == String.self || builderType == String?.self {
      return try ArrowArrayBuilders.loadStringArrayBuilder()
    } else if builderType == Bool.self || builderType == Bool?.self {
      return try ArrowArrayBuilders.loadBoolArrayBuilder()
    } else if builderType == Date.self || builderType == Date?.self {
      return try ArrowArrayBuilders.loadDate64ArrayBuilder()
    } else {
      throw .invalid("Invalid type for builder: \(builderType)")
    }
  }

  public static func isValidBuilderType<T>(_ type: T.Type) -> Bool {
    type == Int8?.self || type == Int16?.self || type == Int32?.self
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
  }

  public static func structArrayBuilderForType<T>(
    _ obj: T
  ) throws -> StructArrayBuilder {
    let mirror = Mirror(reflecting: obj)
    var builders: [AnyArrowArrayBuilder] = []
    var fields: [ArrowField] = []
    for (property, value) in mirror.children {
      guard let propertyName = property else {
        continue
      }
      let builderType = type(of: value)
      let arrowType = try ArrowTypeConverter.infoForType(builderType)
      fields.append(
        ArrowField(
          name: propertyName,
          dataType: arrowType,
          isNullable: true
        )
      )
      builders.append(try loadBuilder(arrowType: arrowType))
    }
    return try StructArrayBuilder(fields, builders: builders)
  }

  public static func loadBuilder(
    arrowType: ArrowType
  ) throws(ArrowError) -> AnyArrowArrayBuilder {
    switch arrowType {
    case .uint8:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<UInt8>
    case .uint16:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<UInt16>
    case .uint32:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<UInt32>
    case .uint64:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<UInt64>
    case .int8:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<Int8>
    case .int16:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<Int16>
    case .int32:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<Int32>
    case .int64:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<Int64>
    case .float16:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<Float16>
    case .float64:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<Double>
    case .float32:
      return try loadNumberArrayBuilder() as NumberArrayBuilder<Float>
    case .utf8:
      return try StringArrayBuilder()
    case .boolean:
      return try BoolArrayBuilder()
    case .binary:
      return try BinaryArrayBuilder()
    case .date32:
      return try Date32ArrayBuilder()
    case .date64:
      return try Date64ArrayBuilder()
    case .time32(let unit):
      return try Time32ArrayBuilder(unit)
    case .time64(let unit):
      return try Time64ArrayBuilder(unit)
    case .timestamp(let unit, _):
      return try TimestampArrayBuilder(unit)
    case .strct(let fields):
      return try StructArrayBuilder(fields)
    case .list(_):
      return try ListArrayBuilder(arrowType)
    default:
      throw ArrowError.unknownType(
        "Builder not found for arrow type: \(arrowType)"
      )
    }
  }

  public static func loadNumberArrayBuilder<T>() throws(ArrowError)
    -> NumberArrayBuilder<T>
  {
    let type = T.self
    if type == Int8.self {
      return try NumberArrayBuilder<T>()
    } else if type == Int16.self {
      return try NumberArrayBuilder<T>()
    } else if type == Int32.self {
      return try NumberArrayBuilder<T>()
    } else if type == Int64.self {
      return try NumberArrayBuilder<T>()
    } else if type == UInt8.self {
      return try NumberArrayBuilder<T>()
    } else if type == UInt16.self {
      return try NumberArrayBuilder<T>()
    } else if type == UInt32.self {
      return try NumberArrayBuilder<T>()
    } else if type == UInt64.self {
      return try NumberArrayBuilder<T>()
    } else if type == Float.self {
      return try NumberArrayBuilder<T>()
    } else if type == Double.self {
      return try NumberArrayBuilder<T>()
    } else {
      throw ArrowError.unknownType("Type is invalid for NumberArrayBuilder")
    }
  }

  public static func loadStringArrayBuilder() throws(ArrowError)
    -> StringArrayBuilder
  {
    try StringArrayBuilder()
  }

  public static func loadBoolArrayBuilder() throws(ArrowError)
    -> BoolArrayBuilder
  {
    try BoolArrayBuilder()
  }

  public static func loadDate32ArrayBuilder() throws(ArrowError)
    -> Date32ArrayBuilder
  {
    try Date32ArrayBuilder()
  }

  public static func loadDate64ArrayBuilder() throws(ArrowError)
    -> Date64ArrayBuilder
  {
    try Date64ArrayBuilder()
  }

  public static func loadBinaryArrayBuilder() throws(ArrowError)
    -> BinaryArrayBuilder
  {
    try BinaryArrayBuilder()
  }

  public static func loadTime32ArrayBuilder(
    _ unit: TimeUnit
  ) throws(ArrowError) -> Time32ArrayBuilder {
    try Time32ArrayBuilder(unit)
  }

  public static func loadTime64ArrayBuilder(
    _ unit: TimeUnit
  ) throws(ArrowError) -> Time64ArrayBuilder {
    try Time64ArrayBuilder(unit)
  }

  public static func loadTimestampArrayBuilder(
    _ unit: TimeUnit,
    timezone: String? = nil
  ) throws -> TimestampArrayBuilder {
    try TimestampArrayBuilder(unit, timezone: timezone)
  }

  public static func loadStructArrayBuilder(
    _ fields: [ArrowField]
  ) throws(ArrowError) -> StructArrayBuilder {
    try StructArrayBuilder(fields)
  }

  public static func loadListArrayBuilder(
    _ elementType: ArrowType
  ) throws(ArrowError) -> ListArrayBuilder {
    try ListArrayBuilder(elementType)
  }
}
