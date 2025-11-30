// ArrowReaderTests.swift
// Arrow
//
// Created by Will Temperley on 29/11/2025. All rights reserved.
// Copyright 2025 Will Temperley.
// 
// Copying or reproduction of this file via any medium requires prior express
// written permission from the copyright holder.
// -----------------------------------------------------------------------------
///
/// Implementation notes, links and internal documentation go here.
///
// -----------------------------------------------------------------------------



import Arrow
import Testing

@testable import ArrowIPC

struct OvertureReaderTests {
  
  @Test func overtureFile() throws {
    
    let url = try loadTestResource(name: "overture_sample")
    print(url)
    let reader = try ArrowReader(url: url)

    let (schema, recordBatches) = try reader.read()
    for recordBatch in recordBatches {
      print(recordBatch)
    }
  }
}
