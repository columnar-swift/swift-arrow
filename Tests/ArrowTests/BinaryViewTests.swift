// BinaryViewTests.swift
// Arrow
//
// Created by Will Temperley on 04/01/2026. All rights reserved.
// Copyright 2026 Will Temperley.
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

@Suite("BinaryView Tests")
struct BinaryViewTests {

  @Test("Inline strings stay inline")
  func inlineStrings() throws {
    let builder = ArrayBuilderBinaryView<String>(maxBufferSize: 2 * 1024 * 1024)

    // All should be inline (≤12 bytes)
    builder.append("hello")  // 5 bytes
    builder.append("world")  // 5 bytes
    builder.append("short")  // 5 bytes

    let array: ArrowArrayBinaryView<String> = builder.finish()

    #expect(array.length == 3)
    #expect(array[0] == "hello")
    #expect(array[1] == "world")
    #expect(array[2] == "short")

    // Should have no data buffers (all inline)
    #expect(array.dataBuffers.count == 0)
    #expect(array.buffers.count == 2)  // null + views only
  }

  @Test("Referenced strings use data buffers")
  func referencedStrings() throws {
    let builder = ArrayBuilderBinaryView<String>(maxBufferSize: 2 * 1024 * 1024)

    // Should be referenced (>12 bytes)
    builder.append("this is a longer string")
    builder.append("another long string here")

    let array = builder.finish()

    #expect(array.length == 2)
    #expect(array[0] == "this is a longer string")
    #expect(array[1] == "another long string here")

    // Should have 1 data buffer
    #expect(array.dataBuffers.count == 1)
    #expect(array.buffers.count == 3)  // null + views + 1 data buffer
  }

  @Test("Mixed inline and referenced strings")
  func mixedInlineAndReferenced() throws {
    let builder = ArrayBuilderBinaryView<String>(maxBufferSize: 2 * 1024 * 1024)

    builder.append("short")  // inline (5 bytes)
    builder.append("this is much longer")  // referenced (19 bytes)
    builder.append("tiny")  // inline (4 bytes)
    builder.appendNull()
    builder.append("123456789012")  // inline (exactly 12 bytes)
    builder.append("1234567890123")  // referenced (13 bytes)

    let array = builder.finish()

    #expect(array.length == 6)
    #expect(array[0] == "short")
    #expect(array[1] == "this is much longer")
    #expect(array[2] == "tiny")
    #expect(array[3] == nil)
    #expect(array[4] == "123456789012")
    #expect(array[5] == "1234567890123")
    #expect(array.nullCount == 1)
    #expect(array.dataBuffers.count == 1)
  }

  @Test("Multiple data buffers with small buffer size")
  func multipleDataBuffers() throws {
    // Small buffer to force rotation
    let builder = ArrayBuilderBinaryView<String>(maxBufferSize: 100)

    // Each string is >12 bytes, so all referenced
    builder.append("first long string value")
    builder.append("second long string value")
    builder.append("third long string value")
    builder.append("fourth long string value")
    builder.append("fifth long string value")

    let array = builder.finish()

    #expect(array.length == 5)
    #expect(array[0] == "first long string value")
    #expect(array[1] == "second long string value")
    #expect(array[2] == "third long string value")
    #expect(array[3] == "fourth long string value")
    #expect(array[4] == "fifth long string value")

    // With 100 byte limit, should have multiple buffers
    #expect(array.dataBuffers.count > 1)
  }

  @Test("Zero-copy slicing shares buffers")
  func zeroCopySlicing() throws {
    let builder = ArrayBuilderBinaryView<String>(maxBufferSize: 2 * 1024 * 1024)

    builder.append("a")
    builder.append("b")
    builder.append("c")
    builder.append("d")
    builder.append("e")

    let array = builder.finish()
    let slice = array.slice(offset: 1, length: 3)

    #expect(slice.length == 3)
    #expect(slice[0] == "b")
    #expect(slice[1] == "c")
    #expect(slice[2] == "d")

    // Slicing should share buffers
    #expect(array.buffers.count == slice.buffers.count)
  }
}
