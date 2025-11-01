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
import Testing

struct ArrayBuilderTests {

  @Test func isValidTypeForBuilder() throws {
    #expect(ArrowArrayBuilders.isValidBuilderType(Int8.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(UInt8.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(UInt8?.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(Int16.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(Int32.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(Int64.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(UInt16.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(UInt32.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(UInt64.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(Float.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(Double.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(Date.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(Bool.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(Int8?.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(Int16?.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(Int32?.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(Int64?.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(UInt16?.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(UInt32?.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(UInt64?.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(Float?.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(Double?.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(Date?.self))
    #expect(ArrowArrayBuilders.isValidBuilderType(Bool?.self))

    #expect(ArrowArrayBuilders.isValidBuilderType(Int.self) == false)
    #expect(ArrowArrayBuilders.isValidBuilderType(UInt.self) == false)
    #expect(ArrowArrayBuilders.isValidBuilderType(Int?.self) == false)
    #expect(ArrowArrayBuilders.isValidBuilderType(UInt?.self) == false)
  }

  @Test func loadArrayBuilders() throws {
    #expect(throws: Never.self) {
      let _ = try ArrowArrayBuilders.builder(for: Int8.self)
      let _ = try ArrowArrayBuilders.builder(for: Int16.self)
      let _ = try ArrowArrayBuilders.builder(for: Int32.self)
      let _ = try ArrowArrayBuilders.builder(for: Int64.self)
      let _ = try ArrowArrayBuilders.builder(for: UInt8.self)
      let _ = try ArrowArrayBuilders.builder(for: UInt16.self)
      let _ = try ArrowArrayBuilders.builder(for: UInt32.self)
      let _ = try ArrowArrayBuilders.builder(for: UInt64.self)
      let _ = try ArrowArrayBuilders.builder(for: Float.self)
      let _ = try ArrowArrayBuilders.builder(for: Double.self)
      let _ = try ArrowArrayBuilders.builder(for: Date.self)
      let _ = try ArrowArrayBuilders.builder(for: Bool.self)
      let _ = try ArrowArrayBuilders.builder(for: Int8?.self)
      let _ = try ArrowArrayBuilders.builder(for: Int16?.self)
      let _ = try ArrowArrayBuilders.builder(for: Int32?.self)
      let _ = try ArrowArrayBuilders.builder(for: Int64?.self)
      let _ = try ArrowArrayBuilders.builder(for: UInt8?.self)
      let _ = try ArrowArrayBuilders.builder(for: UInt16?.self)
      let _ = try ArrowArrayBuilders.builder(for: UInt32?.self)
      let _ = try ArrowArrayBuilders.builder(for: UInt64?.self)
      let _ = try ArrowArrayBuilders.builder(for: Float?.self)
      let _ = try ArrowArrayBuilders.builder(for: Double?.self)
      let _ = try ArrowArrayBuilders.builder(for: Date?.self)
      let _ = try ArrowArrayBuilders.builder(for: Bool?.self)
    }
    #expect(throws: ArrowError.self) {
      let _ = try ArrowArrayBuilders.builder(for: Int.self)
      let _ = try ArrowArrayBuilders.builder(for: UInt.self)
      let _ = try ArrowArrayBuilders.builder(for: Int?.self)
      let _ = try ArrowArrayBuilders.builder(for: UInt?.self)
    }
  }
}
