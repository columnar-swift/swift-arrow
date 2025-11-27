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
