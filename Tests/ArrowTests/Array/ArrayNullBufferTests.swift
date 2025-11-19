// ArrayNullBufferTests.swift
// Arrow
//
// Created by Will Temperley on 19/11/2025. All rights reserved.
// Copyright 2025 Will Temperley.
//
// Copying or reproduction of this file via any medium requires prior express
// written permission from the copyright holder.
// -----------------------------------------------------------------------------
///
/// Implementation notes, links and internal documentation go here.
///
// -----------------------------------------------------------------------------

import Testing

@testable import Arrow

/// Test internal null buffer types are correct.
struct ArrayNullBufferTests {

  @Test func allValidValues() throws {
    // Should be able to omit null buffer entirely
    let arrayBuilder: ArrayBuilderFixedWidth<Int64> = .init()
    for i in 0..<1000 {
      arrayBuilder.append(Int64(i))  // No nulls
    }
    let array = arrayBuilder.finish()
    for i in 0..<1000 {
      #expect(array[i]! == Int64(i))
    }
    let nullBuffer = try #require(array.nullBuffer as? AllValidNullBuffer)
    #expect(nullBuffer.valueCount == 1000)
    #expect(array.bufferSizes == [0, 1000 * MemoryLayout<Int64>.stride])
  }

  @Test func allNullValues() throws {
    let arrayBuilder: ArrayBuilderFixedWidth<Int64> = .init()
    for _ in 0..<1000 {
      arrayBuilder.appendNull()
    }
    let array = arrayBuilder.finish()
    for i in 0..<1000 {
      #expect(array[i] == nil)
    }
    let nullBuffer = try #require(array.nullBuffer as? AllNullBuffer)
    #expect(nullBuffer.valueCount == 1000)
    #expect(array.bufferSizes == [0, 1000 * MemoryLayout<Int64>.stride])
  }
}
