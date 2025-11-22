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
struct ArrowGold: Codable {
  let schema: Schema
  let batches: [Batch]
  let dictionaries: [Dictionary]?

  struct Dictionary: Codable {
    let id: Int
    let data: Batch
  }

  struct DictionaryInfo: Codable {
    let id: Int
    let indexType: FieldType
    let isOrdered: Bool?
  }

  struct Schema: Codable {
    let fields: [Field]
  }

  struct Field: Codable {
    let name: String
    let type: FieldType
    let nullable: Bool
    let children: [Field]?
    let dictionary: DictionaryInfo?
  }

  struct FieldType: Codable {
    let name: String
    let byteWidth: Int?
    let bitWidth: Int?
    let isSigned: Bool?
    let precision: String?
    let scale: Int?
    let unit: String?
    let timezone: String?
  }

  struct Batch: Codable {
    let count: Int
    let columns: [Column]
  }

  struct Column: Codable {
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

  enum Value: Codable {
    case int(Int)
    case string(String)
    case bool(Bool)
  }
}

/// Arrow gold files data values have variable types.
enum DataValue: Codable {
  case string(String)
  case int(Int)
  case double(Double)
  case null

  init(from decoder: Decoder) throws {
    let container = try decoder.singleValueContainer()

    if container.decodeNil() {
      self = .null
    } else if let intValue = try? container.decode(Int.self) {
      self = .int(intValue)
    } else if let doubleValue = try? container.decode(Double.self) {
      self = .double(doubleValue)
    } else if let stringValue = try? container.decode(String.self) {
      self = .string(stringValue)
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
