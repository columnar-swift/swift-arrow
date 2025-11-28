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

/// The JSON file structure used to validate gold-standard  Arrow test files.
struct ArrowGold: Codable, Equatable {
  let schema: Schema
  let batches: [Batch]
  let dictionaries: [Dictionary]?

  struct Dictionary: Codable, Equatable {
    let id: Int
    let data: Batch
  }

  struct DictionaryInfo: Codable, Equatable {
    let id: Int
    let indexType: FieldType
    let isOrdered: Bool?
  }

  struct Schema: Codable, Equatable {
    let fields: [Field]
    let metadata: [KeyValue]?
  }

  struct Field: Codable, Equatable {
    let name: String
    let type: FieldType
    let nullable: Bool
    let children: [Field]?
    let dictionary: DictionaryInfo?
    let metadata: [KeyValue]?
  }

  struct FieldType: Codable, Equatable {
    let name: String
    let byteWidth: Int?
    let bitWidth: Int?
    let isSigned: Bool?
    let precision: String?
    let scale: Int?
    let unit: String?
    let timezone: String?
    let listSize: Int?
  }

  struct Batch: Codable, Equatable {
    let count: Int
    let columns: [Column]
  }

  struct Column: Codable, Equatable {
    let name: String
    let count: Int
    let validity: [Int]?
    let offset: [Int]?
    let data: [DataValue]?
    let children: [Column]?

    enum CodingKeys: String, CodingKey {
      case name
      case count
      case validity = "VALIDITY"
      case offset = "OFFSET"
      case data = "DATA"
      case children
    }
  }

  enum Value: Codable, Equatable {
    case int(Int)
    case string(String)
    case bool(Bool)
  }
}

/// A metadata key-value entry.
struct KeyValue: Codable, Equatable {
  let key: String
  let value: String
}

extension [KeyValue] {
  var asDictionary: [String: String] {
    Dictionary(uniqueKeysWithValues: self.map { ($0.key, $0.value) })
  }
}

/// Arrow gold files data values have variable types.
enum DataValue: Codable, Equatable {
  case string(String)
  case int(Int)
  case bool(Bool)
  case null

  init(from decoder: Decoder) throws {
    let container = try decoder.singleValueContainer()

    if container.decodeNil() {
      self = .null
    } else if let intValue = try? container.decode(Int.self) {
      self = .int(intValue)
    } else if let doubleValue = try? container.decode(Double.self) {
      self = .string(String(doubleValue))
    } else if let stringValue = try? container.decode(String.self) {
      self = .string(stringValue)
    } else if let boolValue = try? container.decode(Bool.self) {
      self = .bool(boolValue)
    } else {
      throw DecodingError.typeMismatch(
        DataValue.self,
        DecodingError.Context(
          codingPath: decoder.codingPath,
          debugDescription: "Cannot decode DataValue")
      )
    }
  }
}

extension ArrowGold.Column {

  /// Filter for the valid values.
  /// - Returns: The test column data with nulls in place of junk values.
  func withoutJunkData() -> Self {
    guard let data = self.data, let validity = self.validity else {
      return self
    }
    let filteredData = data.enumerated().map { index, value in
      validity[index] == 1 ? value : .null
    }
    return Self(
      name: name,
      count: count,
      validity: validity,
      offset: offset,
      data: filteredData,
      //      data: filteredData.isEmpty ? nil : filteredData,
      children: children?.map { $0.withoutJunkData() }
    )
  }
}
