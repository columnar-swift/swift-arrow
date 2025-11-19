# Swift Arrow

![Swift 6.2](https://img.shields.io/badge/Swift-6.2-orange?style=for-the-badge&logo=swift&logoColor=white)

A Swift implementation of Apache Arrow, the universal columnar format for fast data interchange and in-memory analytics.

This is a **work in progress**. Do not use in production. Progress is fast however, expect a beta in December.

## Array interface

Arrow arrays are backed by a standard memory layout:
https://arrow.apache.org/docs/format/Columnar.html

In Swift-Arrow, every array conforms to: 

```swift
public protocol ArrowArrayProtocol {
  associatedtype ItemType
  subscript(_ index: Int) -> ItemType? { get }
  var offset: Int { get }
  var length: Int { get }
  func slice(offset: Int, length: Int) -> Self
  func any(at index: Int) -> Any?
}
```

The in-memory contiguous buffers allow constant-time random access.

Every Arrow array supports nullable elements. This is encoded as an optional bit-packed validity buffer aka null array aka bitfield. 
In psuedocode, bitfield[index] == 0 means null or invalid, and bitfield[index] == 1 means not null or valid.
Fixed-width types are encoded back-to-back, with placeholder values for nulls. For example the array:

```swift
let swiftArray: [Int8?] = [1, nil, 2, 3, nil, 4]
let arrayBuilder: ArrayBuilderFixedWidth<Int8> = .init()
for value in swiftArray {
  if let value {
    arrayBuilder.append(value)
  } else {
    arrayBuilder.appendNull()
  }
}
let arrowArray = arrayBuilder.finish()
for i in 0..<swiftArray.count {
  #expect(arrowArray[i] == swiftArray[i])
}
```

would be backed by a values buffer of `Int8`:

`[1, 0, 2, 3, 0, 4]`

and a bit-packed validity buffer of UInt8:
`[45]` or `[b00101101]`

Note the validity buffer may be empty if all values are null, or all values are non null.

Arrow Arrays of variable-length types such as `String` have an offsets buffer. For example:

```swift
let swiftArray: [String?] = ["ab", nil, "c", "", "."]
let arrayBuilder: ArrayBuilderVariable<String> = .init()
for value in swiftArray {
  if let value {
    arrayBuilder.append(value)
  } else {
    arrayBuilder.appendNull()
  }
}
let arrowArray = arrayBuilder.finish()
#expect(arrowArray[0] == "ab")
#expect(arrowArray[1] == nil)
#expect(arrowArray[2] == "c")
#expect(arrowArray[3] == "")
#expect(arrowArray[4] == ".")
```

would have an offsets array of array length + 1 integers:
`[0, 2, 2, 3, 3, 4]`

This is a lookup into the value array, i.e.:

```swift
let values: [UInt8] = [97, 98, 99, 46]
print(values[0..<2]) // [97, 98]
print(values[2..<2]) // []
print(values[2..<3]) // [99]
print(values[3..<4]) // [46]
```

In practice, buffers can be any contingous storage. In Swift-Arrow, arrays created in memory are usually backed by pointers, whereas arrays loaded from IPC files are backed by memory-mapped `Data` instances.

Arrays can be configured to use different buffer types, by specifying the types as 
`public struct ArrowArrayVariable<OffsetsBuffer, ValueBuffer>`

this allows the buffer types to be user-specified, e.g.:
```
typealias ArrowArrayUtf8 = ArrowArrayVariable<
  FixedWidthBufferIPC<Int32>,
  VariableLengthBufferIPC<String>
>
``


## Relationship to Arrow-Swift

This project is based on Arrow-Swift, the official Swift implementation of Apache Arrow. The decision was made to at least temporarily operate independently of the Apache Software Foundation (ASF). Currently there are no active ASF maintaners with knowledge of Swift, and the only [Apache approved CI for Swift](https://github.com/apache/infrastructure-actions/blob/main/approved_patterns.yml) is [setup-swift which is unmaintained](https://github.com/swift-actions/setup-swift/issues), leading to intermittent CI failures. This has led to delays in much-needed fixes being implemented.

The intention is to continue contributing to the official Apache-Swift repository, however changes can be iterated on more quickly here.

Original source: https://github.com/apache/arrow-swift

Changes made since forking Arrow-Swift:
* CI uses the swiftlang workflows: https://github.com/swiftlang/github-workflows
* `ArrowType` has been moved from a class hierarchy to an enum to improve concurrency support.
* Tests have been migrated to Swift Testing.
* A migration from reference to value types, where appropriate, has begun.
* A DockerFile for compiling ArrowFlight protocol buffers and grpc classes is provided.
* C export has been made Swift 6 compatible through MainActor annotations. This is a workaround.
