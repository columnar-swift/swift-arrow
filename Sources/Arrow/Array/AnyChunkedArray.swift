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


// Type-erased chunked array for complex types (List, Struct, Map, Union)
public final class AnyChunkedArray: ChunkedArrayProtocol {
    private let arrays: [any AnyArrowArrayProtocol]
    public let nullCount: Int
    public let length: Int
    
    // Cached chunk boundaries
    private let chunkOffsets: [Int]
    
    
    public init(_ arrays: [any AnyArrowArrayProtocol]) throws(ArrowError) {
        guard !arrays.isEmpty else {
            throw ArrowError(.arrayHasNoElements)
        }
        
        var len: Int = 0
        var nullCount: Int = 0
        var offsets: [Int] = [0]
        
        for array in arrays {
            len += array.length
            nullCount += array.nullCount
            offsets.append(len)
        }
        
        self.arrays = arrays
        self.length = len
        self.nullCount = nullCount
        self.chunkOffsets = offsets
        
    }
    
    public subscript(_ index: Int) -> Any? {
        guard index >= 0, index < length else {
            return nil
        }
        
        // Binary search to find the right chunk
        var low = 0
        var high = arrays.count - 1
        
        while low <= high {
            let mid = (low + high) / 2
            let chunkStart = chunkOffsets[mid]
            let chunkEnd = chunkOffsets[mid + 1]
            
            if index < chunkStart {
                high = mid - 1
            } else if index >= chunkEnd {
                low = mid + 1
            } else {
                // Found the right chunk
                let localIndex = index - chunkStart
                return arrays[mid].any(at: localIndex)
            }
        }
        
        return nil
    }
    
    public func any(at index: Int) -> Any? {
        return self[index]
    }
    
    
    public func asString(_ index: Int) -> String {
        guard let value = self[index] else {
            return ""
        }
        return "\(value)"
    }
}

// Helper to create appropriate ChunkedArray type
