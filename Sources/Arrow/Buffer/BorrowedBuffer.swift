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

class BorrowedBuffer<T> {

  let data: Data
  let range: Range<Int>

  init(borrowing data: Data, range: Range<Int>) {
    self.data = data
    self.range = range
  }
}

class BorrowedOffsets: BorrowedBuffer<UInt32>, OffsetsBuffer {

  var count: Int

  init(count: Int, data: Data, range: Range<Int>) {
    // Offsets are fenceposts.
    precondition(count == range.count / MemoryLayout<UInt32>.stride - 1)
    self.count = count
    super.init(borrowing: data, range: range)
  }

  func offsets(at index: Int) -> (start: Int32, end: Int32) {
    precondition(index < count, "Index out of range")
    return data.bytes.withUnsafeBytes { rawBuffer in
      let sub = rawBuffer[range]
      let span = Span<Int32>(_unsafeBytes: sub)
      let start = index > 0 ? span[index] : 0
      let end = span[index + 1]
      return (start, end)
    }
  }
}
