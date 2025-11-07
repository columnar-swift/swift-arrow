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

/// A type which provides offset ranges in Arrow arrays.
protocol OffsetsBuffer {
  /// Number of offset pairs available
  var count: Int { get }

  /// Get the start and end offsets for the element at index
  /// - Parameter index: Zero-based index of the element
  /// - Returns: Tuple of (start, end) offsets
  func offsets(at index: Int) -> (start: Int32, end: Int32)

  /// Get just the length for the element at index
  /// - Parameter index: Zero-based index of the element
  /// - Returns: Length in bytes/elements
  func length(at index: Int) -> Int32
}

extension OffsetsBuffer {
  func length(at index: Int) -> Int32 {
    let (start, end) = offsets(at: index)
    return end - start
  }
}
