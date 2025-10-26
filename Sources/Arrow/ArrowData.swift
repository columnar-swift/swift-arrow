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

public class ArrowData {
  public let type: ArrowType
  public let buffers: [ArrowBuffer]
  public let children: [ArrowData]
  public let nullCount: UInt
  public let length: UInt
  public let stride: Int

  convenience init(
    _ arrowType: ArrowType,
    buffers: [ArrowBuffer],
    nullCount: UInt
  ) throws(ArrowError) {
    try self.init(
      arrowType, buffers: buffers,
      children: [ArrowData](), nullCount: nullCount,
      length: buffers[1].length)
  }

  init(
    _ arrowType: ArrowType,
    buffers: [ArrowBuffer],
    children: [ArrowData],
    nullCount: UInt,
    length: UInt
  ) throws(ArrowError) {
    let infoType = arrowType.info
    switch infoType {
    case .primitiveInfo(let typeId):
      if typeId == ArrowTypeId.unknown {
        throw ArrowError.unknownType("Unknown primitive type for data")
      }
    case .variableInfo(let typeId):
      if typeId == ArrowTypeId.unknown {
        throw ArrowError.unknownType("Unknown variable type for data")
      }
    case .timeInfo(let typeId):
      if typeId == ArrowTypeId.unknown {
        throw ArrowError.unknownType("Unknown time type for data")
      }
    case .complexInfo(let typeId):
      if typeId == ArrowTypeId.unknown {
        throw ArrowError.unknownType("Unknown complex type for data")
      }
    }

    self.type = arrowType
    self.buffers = buffers
    self.children = children
    self.nullCount = nullCount
    self.length = length
    self.stride = arrowType.getStride()
  }

  public func isNull(_ at: UInt) -> Bool {
    let nullBuffer = buffers[0]
    return nullBuffer.length > 0 && !BitUtility.isSet(at, buffer: nullBuffer)
  }
}
