# Swift Arrow

![Swift 6.2](https://img.shields.io/badge/Swift-6.2-orange?style=for-the-badge&logo=swift&logoColor=white)

A Swift implementation of Apache Arrow, the universal columnar format for fast data interchange and in-memory analytics.

This is an independent Swift-focused project. It implements the Apache Arrow specification and is fully compatible with other Arrow implementations.

This project is based on Arrow-Swift and maintains specification compatibility. The decision was made to at least temporarily operate independently of the Apache Software Foundation (ASF). Currently there are no active ASF maintaners with knowledge of Swift, and the only [Apache approved CI for Swift](https://github.com/apache/infrastructure-actions/blob/main/approved_patterns.yml) is [setup-swift which is unmaintained](https://github.com/swift-actions/setup-swift/issues), leading to intermittent CI failures. This has led to delays in much-needed fixes being implemented.
Original source: https://github.com/apache/arrow-swift

Changes made since forking Arrow-Swift:
* Moved to the swiftlang workflows: https://github.com/swiftlang/github-workflows
* `ArrowType` has been moved from a class hierarchy to an enum.
* A gradual migration from classes to structs, where appropriate, has begun.
