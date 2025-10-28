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

public protocol ArrowArrayHolderBuilder {
  func toHolder() throws(ArrowError) -> ArrowArrayHolder
  func appendAny(_ val: Any?)
}

public class ArrowArrayBuilder<
  T: ArrowBufferBuilder, U: ArrowArray<T.ItemType>
>:
  ArrowArrayHolderBuilder
{
  let type: ArrowType
  let bufferBuilder: T
  public var length: UInt { self.bufferBuilder.length }
  public var capacity: UInt { self.bufferBuilder.capacity }
  public var nullCount: UInt { self.bufferBuilder.nullCount }
  public var offset: UInt { self.bufferBuilder.offset }

  fileprivate init(_ type: ArrowType) throws(ArrowError) {
    self.type = type
    self.bufferBuilder = try T()
  }

  public func append(_ vals: T.ItemType?...) {
    for val in vals {
      self.bufferBuilder.append(val)
    }
  }

  public func append(_ vals: [T.ItemType?]) {
    for val in vals {
      self.bufferBuilder.append(val)
    }
  }

  public func append(_ val: T.ItemType?) {
    self.bufferBuilder.append(val)
  }

  public func appendAny(_ val: Any?) {
    self.bufferBuilder.append(val as? T.ItemType)
  }

  public func finish() throws(ArrowError) -> ArrowArray<T.ItemType> {
    let buffers = self.bufferBuilder.finish()
    let arrowData = try ArrowData(
      self.type,
      buffers: buffers,
      nullCount: self.nullCount
    )
    let array = try U(arrowData)
    return array
  }

  public func getStride() -> Int {
    self.type.getStride()
  }

  public func toHolder() throws(ArrowError) -> ArrowArrayHolder {
    try ArrowArrayHolderImpl(self.finish())
  }
}

public class NumberArrayBuilder<T>: ArrowArrayBuilder<
  FixedBufferBuilder<T>, FixedArray<T>
>
where T: Numeric {
  fileprivate convenience init() throws(ArrowError) {
    try self.init(try ArrowTypeConverter.infoForNumericType(T.self))
  }
}

public class StringArrayBuilder: ArrowArrayBuilder<
  VariableBufferBuilder<String>, StringArray
>
{
  fileprivate convenience init() throws(ArrowError) {
    try self.init(.utf8)
  }
}

public class BinaryArrayBuilder: ArrowArrayBuilder<
  VariableBufferBuilder<Data>, BinaryArray
>
{
  fileprivate convenience init() throws(ArrowError) {
    try self.init(.binary)
  }
}

public class BoolArrayBuilder: ArrowArrayBuilder<BoolBufferBuilder, BoolArray> {
  fileprivate convenience init() throws(ArrowError) {
    try self.init(.boolean)
  }
}

public class Date32ArrayBuilder: ArrowArrayBuilder<
  Date32BufferBuilder, Date32Array
>
{
  fileprivate convenience init() throws(ArrowError) {
    try self.init(.date32)
  }
}

public class Date64ArrayBuilder: ArrowArrayBuilder<
  Date64BufferBuilder, Date64Array
>
{
  fileprivate convenience init() throws(ArrowError) {
    try self.init(.date64)
  }
}

public class Time32ArrayBuilder: ArrowArrayBuilder<
  FixedBufferBuilder<Time32>, Time32Array
>
{
  fileprivate convenience init(_ unit: TimeUnit) throws(ArrowError) {
    try self.init(.time32(unit))
  }
}

public class Time64ArrayBuilder: ArrowArrayBuilder<
  FixedBufferBuilder<Time64>, Time64Array
>
{
  fileprivate convenience init(_ unit: TimeUnit) throws(ArrowError) {
    try self.init(.time64(unit))
  }
}

public class TimestampArrayBuilder: ArrowArrayBuilder<
  FixedBufferBuilder<Int64>, TimestampArray
>
{
  fileprivate convenience init(
    _ unit: TimeUnit, timezone: String? = nil
  ) throws(ArrowError) {
    try self.init(.timestamp(unit, timezone))
  }
}

public class StructArrayBuilder: ArrowArrayBuilder<
  StructBufferBuilder, NestedArray
>
{
  let builders: [any ArrowArrayHolderBuilder]
  let fields: [ArrowField]
  public init(_ fields: [ArrowField], builders: [any ArrowArrayHolderBuilder])
    throws(ArrowError)
  {
    self.fields = fields
    self.builders = builders
    try super.init(.strct(fields))
    self.bufferBuilder.initializeTypeInfo(fields)
  }

  public init(_ fields: [ArrowField]) throws(ArrowError) {
    self.fields = fields
    var builders: [any ArrowArrayHolderBuilder] = []
    for field in fields {
      builders.append(
        try ArrowArrayBuilders.loadBuilder(arrowType: field.type))
    }
    self.builders = builders
    try super.init(.strct(fields))
  }

  public override func append(_ values: [Any?]?) {
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

  public override func finish() throws(ArrowError) -> NestedArray {
    let buffers = self.bufferBuilder.finish()
    var childData: [ArrowData] = []
    for builder in self.builders {
      childData.append(try builder.toHolder().array.arrowData)
    }

    let arrowData = try ArrowData(
      self.type, buffers: buffers,
      children: childData, nullCount: self.nullCount,
      length: self.length)
    let structArray = try NestedArray(arrowData)
    return structArray
  }
}

public class ListArrayBuilder: ArrowArrayBuilder<ListBufferBuilder, NestedArray>
{
  let valueBuilder: any ArrowArrayHolderBuilder

  public override init(_ elementType: ArrowType) throws(ArrowError) {

    guard case .list(let field) = elementType else {
      throw .invalid("Expected a field with type .list")
    }

    self.valueBuilder = try ArrowArrayBuilders.loadBuilder(
      arrowType: field.type
    )
    try super.init(elementType)
  }

  public override func append(_ values: [Any?]?) {
    self.bufferBuilder.append(values)
    if let vals = values {
      for val in vals {
        self.valueBuilder.appendAny(val)
      }
    }
  }

  public override func finish() throws(ArrowError) -> NestedArray {
    let buffers = self.bufferBuilder.finish()
    let childData = try valueBuilder.toHolder().array.arrowData
    let arrowData = try ArrowData(
      self.type,
      buffers: buffers,
      children: [childData],
      nullCount: self.nullCount,
      length: self.length
    )
    return try NestedArray(arrowData)
  }
}

public class ArrowArrayBuilders {
  public static func loadBuilder(
    _ builderType: Any.Type
  ) throws(ArrowError) -> ArrowArrayHolderBuilder {
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

  public static func loadStructArrayBuilderForType<T>(
    _ obj: T
  ) throws -> StructArrayBuilder {
    let mirror = Mirror(reflecting: obj)
    var builders: [ArrowArrayHolderBuilder] = []
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
  ) throws(ArrowError) -> ArrowArrayHolderBuilder {
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

  public static func loadTime32ArrayBuilder(_ unit: TimeUnit)
    throws(ArrowError) -> Time32ArrayBuilder
  {
    try Time32ArrayBuilder(unit)
  }

  public static func loadTime64ArrayBuilder(_ unit: TimeUnit)
    throws(ArrowError) -> Time64ArrayBuilder
  {
    try Time64ArrayBuilder(unit)
  }

  public static func loadTimestampArrayBuilder(
    _ unit: TimeUnit, timezone: String? = nil
  )
    throws -> TimestampArrayBuilder
  {
    try TimestampArrayBuilder(unit, timezone: timezone)
  }

  public static func loadStructArrayBuilder(_ fields: [ArrowField])
    throws(ArrowError) -> StructArrayBuilder
  {
    try StructArrayBuilder(fields)
  }

  public static func loadListArrayBuilder(_ elementType: ArrowType)
    throws(ArrowError) -> ListArrayBuilder
  {
    try ListArrayBuilder(elementType)
  }
}
