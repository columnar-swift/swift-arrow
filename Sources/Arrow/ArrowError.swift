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

public struct ArrowError: Error {

  public enum ErrorType: Equatable, Sendable {
    case none
    case unknownType(String)
    case runtimeError(String)
    case outOfBounds(index: Int64)
    case arrayHasNoElements
    case unknownError(String)
    case notImplemented
    case ioError(String)
    case invalid(String)
  }

  let type: ErrorType
  let underlyingError: Error?

  public init(_ type: ErrorType, underlyingError: Error? = nil) {
    self.type = type
    self.underlyingError = underlyingError
  }
}
