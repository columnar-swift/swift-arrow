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

// Note this is a reference type to reduce copying.
public final class ArrowSchema: Sendable {
  public let fields: [ArrowField]
  public let fieldLookup: [String: Int]
  let metadata: [String: String]?

  public init(_ fields: [ArrowField], metadata: [String: String]? = nil) {
    var fieldLookup: [String: Int] = [:]
    for (index, field) in fields.enumerated() {
      fieldLookup[field.name] = index
    }

    self.fields = fields
    self.fieldLookup = fieldLookup
    self.metadata = metadata
  }

  public func field(_ index: Int) -> ArrowField {
    self.fields[index]
  }

  public func fieldIndex(_ name: String) -> Int? {
    self.fieldLookup[name]
  }

  public class Builder {
    private var fields: [ArrowField] = []

    public init() {}

    @discardableResult
    public func addField(_ field: ArrowField) -> Builder {
      fields.append(field)
      return self
    }

    @discardableResult
    public func addField(
      _ name: String,
      type: ArrowType,
      isNullable: Bool
    ) -> Builder {
      fields.append(
        ArrowField(name: name, dataType: type, isNullable: isNullable))
      return self
    }

    public func finish() -> ArrowSchema {
      ArrowSchema(fields)
    }
  }
}
