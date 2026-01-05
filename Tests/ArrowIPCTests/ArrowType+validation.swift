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

import Arrow

extension ArrowType {

  /// Recursively check this type matches the expected field type..
  /// - Parameter expectedField: The Arrow integration test field.
  /// - Returns: True if this type and the field match exactly.
  func matches(expectedField: ArrowGold.Field) -> Bool {
    let fieldType = expectedField.type
    switch self {
    case .int8:
      return fieldType.name == "int" && fieldType.bitWidth == 8
        && fieldType.isSigned == true
    case .int16:
      return fieldType.name == "int" && fieldType.bitWidth == 16
        && fieldType.isSigned == true
    case .int32:
      return fieldType.name == "int" && fieldType.bitWidth == 32
        && fieldType.isSigned == true
    case .int64:
      return fieldType.name == "int" && fieldType.bitWidth == 64
        && fieldType.isSigned == true
    case .uint8:
      return fieldType.name == "int" && fieldType.bitWidth == 8
        && fieldType.isSigned == false
    case .uint16:
      return fieldType.name == "int" && fieldType.bitWidth == 16
        && fieldType.isSigned == false
    case .uint32:
      return fieldType.name == "int" && fieldType.bitWidth == 32
        && fieldType.isSigned == false
    case .uint64:
      return fieldType.name == "int" && fieldType.bitWidth == 64
        && fieldType.isSigned == false
    case .float16:
      return fieldType.name == "floatingpoint" && fieldType.precision == "HALF"
    case .float32:
      return fieldType.name == "floatingpoint"
        && fieldType.precision == "SINGLE"
    case .float64:
      return fieldType.name == "floatingpoint"
        && fieldType.precision == "DOUBLE"
    case .boolean:
      return fieldType.name == "bool"
    case .utf8:
      return fieldType.name == "utf8"
    case .binary:
      return fieldType.name == "binary"
    case .fixedSizeBinary(let byteWidth):
      guard let expectedByteWidth = fieldType.byteWidth else {
        fatalError("FieldType does not contain byteWidth.")
      }
      return fieldType.name == "fixedsizebinary"
        && expectedByteWidth == byteWidth
    case .date32:
      return fieldType.name == "date" && fieldType.unit == "DAY"
    case .date64:
      return fieldType.name == "date" && fieldType.unit == "MILLISECOND"
    case .timestamp(let unit, let timezone):
      return fieldType.name == "timestamp" && fieldType.unit == unit.jsonName
        && fieldType.timezone == timezone
    case .time32(let unit):
      return fieldType.name == "time" && fieldType.unit == unit.jsonName
        && fieldType.bitWidth == 32
    case .time64(let unit):
      return fieldType.name == "time" && fieldType.unit == unit.jsonName
        && fieldType.bitWidth == 64
    case .duration(let unit):
      return fieldType.name == "duration" && fieldType.unit == unit.jsonName
    case .decimal128(let precision, let scale):
      guard let expectedScale = fieldType.scale else {
        fatalError("FieldType does not contain scale.")
      }
      return fieldType.name == "decimal" && fieldType.bitWidth == 128
        && fieldType.precision == String(precision) && expectedScale == scale
    case .decimal256(let precision, let scale):
      guard let expectedScale = fieldType.scale else {
        fatalError("FieldType does not contain scale.")
      }
      return fieldType.name == "decimal" && fieldType.bitWidth == 256
        && fieldType.precision == String(precision) && expectedScale == scale
    case .list(let arrowField), .largeList(let arrowField):

      guard fieldType.name == "list" || fieldType.name == "largelist",
        let children = expectedField.children,
        children.count == 1
      else {
        return false
      }
      return arrowField.type.matches(expectedField: children[0])
    case .fixedSizeList(let arrowField, let listSize):
      guard fieldType.name == "fixedsizelist",
        let children = expectedField.children,
        children.count == 1,
        let expectedListSize = fieldType.listSize,
        expectedListSize == listSize
      else {
        return false
      }
      return arrowField.type.matches(expectedField: children[0])
    case .strct(let arrowFields):
      guard fieldType.name == "struct", let children = expectedField.children
      else {
        return false
      }
      for (arrowField, child) in zip(arrowFields, children) {
        let matches = arrowField.type.matches(expectedField: child)
        if !matches {
          return false
        }
      }
      return true
    case .map:
      //      return fieldType.name == self.jsonTypeName
      fatalError("Not implemented.")

    default:
      fatalError("Not implemented.")
    }
  }

  var jsonTypeName: String {
    switch self {
    case .list: return "list"
    case .largeList: return "largelist"
    case .fixedSizeList: return "fixedsizelist"
    case .strct: return "struct"
    case .map: return "map"
    default: fatalError("Not a container type")
    }
  }
}

extension TimeUnit {
  var jsonName: String {
    switch self {
    case .second: return "SECOND"
    case .millisecond: return "MILLISECOND"
    case .microsecond: return "MICROSECOND"
    case .nanosecond: return "NANOSECOND"
    }
  }
}

extension ArrowField {
  func toGoldField() -> ArrowGold.Field {
    ArrowGold.Field(
      name: name,
      type: type.toGoldFieldType(),
      nullable: isNullable,
      children: type.goldChildren(),
      dictionary: nil,  // TODO: handle dictionary encoding if needed
      metadata: self.metadata.isEmpty ? nil : self.metadata
    )
  }
}

extension ArrowType {
  func toGoldFieldType() -> ArrowGold.FieldType {
    let name: String
    var byteWidth: Int?
    var bitWidth: Int?
    var isSigned: Bool? = nil
    var precision: String? = nil
    var scale: Int? = nil
    var unit: String? = nil
    var timezone: String? = nil
    var listSize: Int? = nil

    switch self {
    case .int8:
      name = "int"
      bitWidth = 8
      isSigned = true
    case .int16:
      name = "int"
      bitWidth = 16
      isSigned = true
    case .int32:
      name = "int"
      bitWidth = 32
      isSigned = true
    case .int64:
      name = "int"
      bitWidth = 64
      isSigned = true
    case .uint8:
      name = "int"
      bitWidth = 8
      isSigned = false
    case .uint16:
      name = "int"
      bitWidth = 16
      isSigned = false
    case .uint32:
      name = "int"
      bitWidth = 32
      isSigned = false
    case .uint64:
      name = "int"
      bitWidth = 64
      isSigned = false
    case .float16:
      name = "floatingpoint"
      precision = "HALF"
    case .float32:
      name = "floatingpoint"
      precision = "SINGLE"
    case .float64:
      name = "floatingpoint"
      precision = "DOUBLE"
    case .boolean:
      name = "bool"
    case .binary:
      name = "binary"
    case .utf8:
      name = "utf8"
    case .binaryView:
      name = "binaryview"
    case .utf8View:
      name = "utf8view"
    case .fixedSizeBinary(let byteWidth_):
      byteWidth = Int(byteWidth_)
      name = "fixedsizebinary"
    case .date32:
      name = "date"
      unit = "DAY"
    case .date64:
      name = "date"
      unit = "MILLISECOND"
    case .timestamp(let unit_, let timezone_):
      name = "timestamp"
      unit = unit_.jsonName
      timezone = timezone_
    case .time32(let unit_):
      name = "time"
      bitWidth = 32
      unit = unit_.jsonName
    case .time64(let unit_):
      name = "time"
      bitWidth = 64
      unit = unit_.jsonName
    case .duration(let unit_):
      name = "duration"
      bitWidth = nil
      unit = unit_.jsonName
    case .decimal128(let precision_, let scale_):
      name = "decimal"
      bitWidth = 128
      precision = String(precision_)
      scale = Int(scale_)
    case .decimal256(let precision_, let scale_):
      name = "decimal"
      bitWidth = 256
      precision = String(precision_)
      scale = Int(scale_)
    case .list(_):
      name = "list"
    case .largeList(_):
      name = "largelist"
    case .fixedSizeList(_, let listSize_):
      name = "fixedsizelist"
      listSize = Int(listSize_)
    case .strct(_):
      name = "struct"
    case .map:
      name = "map"
    default:
      fatalError("Unhandled type: \(self)")
    }
    return ArrowGold.FieldType(
      name: name,
      byteWidth: byteWidth,
      bitWidth: bitWidth,
      isSigned: isSigned,
      precision: precision,
      scale: scale,
      unit: unit,
      timezone: timezone,
      listSize: listSize
    )
  }

  func goldChildren() -> [ArrowGold.Field]? {
    switch self {
    case .list(let field), .largeList(let field), .fixedSizeList(let field, _),
      .map(let field, _):
      return [field.toGoldField()]
    case .strct(let fields):
      return fields.map { $0.toGoldField() }
    default:
      // May need to implement different nested types.
      if isNested { fatalError("Not implemented for nested ArrowType") }
      return []
    }
  }
}
