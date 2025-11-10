// Derived from swift-binary-parsing (https://github.com/apple/swift-binary-parsing)
// Copyright (c) 2025 Apple Inc. and the Swift project authors
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

/// The random seed to use for the RNG when "fuzzing", calculated once per
/// testing session.
let randomSeed = {
  let seed = UInt64.random(in: .min ... .max)
  print(
    "let randomSeed = 0x\(String(seed, radix: 16)) as UInt64 // Fuzzing seed")
  return seed
}()

/// The count for iterations when "fuzzing".
var fuzzIterationCount: Int { 100 }

/// Returns an RNG that is seeded with `randomSeed`.
func getSeededRNG(named name: String = #function) -> some RandomNumberGenerator
{
  RapidRandom(seed: randomSeed)
}

/// A seeded random number generator type.
struct RapidRandom: RandomNumberGenerator {
  private var state: UInt64

  static func mix(_ a: UInt64, _ b: UInt64) -> UInt64 {
    let result = a.multipliedFullWidth(by: b)
    return result.low ^ result.high
  }

  init(seed: UInt64) {
    self.state =
      seed ^ Self.mix(seed ^ 0x2d35_8dcc_aa6c_78a5, 0x8bb8_4b93_962e_acc9)
  }

  @inlinable
  mutating func next() -> UInt64 {
    state &+= 0x2d35_8dcc_aa6c_78a5
    return Self.mix(state, state ^ 0x8bb8_4b93_962e_acc9)
  }
}
