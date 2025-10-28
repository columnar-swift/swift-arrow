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

import Arrow
import Foundation

public struct FlightSchemaResult: Sendable {
  let schemaResult: Arrow_Flight_Protocol_SchemaResult

  public var schema: ArrowSchema? {
    do {
      return try schemaFromMessage(self.schemaResult.schema)
    } catch {
      fatalError()  // FIXME: this was traded for removing force-unwraps
    }
  }

  public init(_ schema: Data) {
    self.schemaResult = Arrow_Flight_Protocol_SchemaResult.with {
      $0.schema = schema
    }
  }

  init(_ schemaResult: Arrow_Flight_Protocol_SchemaResult) {
    self.schemaResult = schemaResult
  }

  func toProtocol() -> Arrow_Flight_Protocol_SchemaResult {
    schemaResult
  }
}
