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

public class ArrowColumn {
  let dataHolder: any ChunkedArrayProtocol
  public let field: ArrowField
  public var length: Int { self.dataHolder.length }
  public var nullCount: Int { self.dataHolder.nullCount }

  public var name: String { field.name }
  
  public init(_ field: ArrowField, chunked: any ChunkedArrayProtocol) {
    self.field = field
    self.dataHolder = chunked
  }

  public func data<T>() throws(ArrowError) -> ChunkedArray<T> {
    if let holder = self.dataHolder as? ChunkedArray<T> {
      return holder
    } else {
      throw .init(
        .runtimeError("Could not cast array holder to chunked array."))
    }
  }
}
