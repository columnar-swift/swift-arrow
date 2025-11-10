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

/// A buffer providing offsets, backed by an `ArrowBuffer` temporarily.
struct ArrowBufferBackedOffsets: OffsetsBuffer {

  let arrowBuffer: ArrowBuffer

  init(_ arrowBuffer: ArrowBuffer) {
    self.arrowBuffer = arrowBuffer
  }

  func offsets(at index: Int) -> (start: Int32, end: Int32) {

    let offsets = arrowBuffer
    let offsetIndex = MemoryLayout<Int32>.stride * Int(index)
    var startIndex: Int32 = 0
    if index > 0 {
      startIndex = offsets.rawPointer.advanced(by: offsetIndex)
        .load(as: Int32.self)
    }
    let endIndex = offsets.rawPointer.advanced(
      by: offsetIndex + MemoryLayout<Int32>.stride
    )
    .load(as: Int32.self)

    return (start: startIndex, end: endIndex)
  }
}
