// Copyright 2025 The Apache Software Foundation
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

/// The maximum precision for [DataType::Decimal32] values
public let DECIMAL32_MAX_PRECISION: UInt8 = 9

/// The maximum scale for [DataType::Decimal32] values
public let DECIMAL32_MAX_SCALE: Int8 = 9

/// The maximum precision for [DataType::Decimal64] values
public let DECIMAL64_MAX_PRECISION: UInt8 = 18

/// The maximum scale for [DataType::Decimal64] values
public let DECIMAL64_MAX_SCALE: Int8 = 18

/// The maximum precision for [DataType::Decimal128] values
public let DECIMAL128_MAX_PRECISION: UInt8 = 38

/// The maximum scale for [DataType::Decimal128] values
public let DECIMAL128_MAX_SCALE: Int8 = 38

/// The maximum precision for [DataType::Decimal256] values
public let DECIMAL256_MAX_PRECISION: UInt8 = 76

/// The maximum scale for [DataType::Decimal256] values
public let DECIMAL256_MAX_SCALE: Int8 = 76

/// The default scale for [DataType::Decimal32] values
public let DECIMAL32_DEFAULT_SCALE: Int8 = 2

/// The default scale for [DataType::Decimal64] values
public let DECIMAL64_DEFAULT_SCALE: Int8 = 6

/// The default scale for [DataType::Decimal128] and [DataType::Decimal256]
/// values
public let DECIMAL_DEFAULT_SCALE: Int8 = 10

/// The metadata key for the string name identifying an [`ExtensionType`].
public let extensionTypeNameKey = "ARROW:extension:name"

/// The metadata key for a serialized representation of the [`ExtensionType`]
/// necessary to reconstruct the custom type.
public let extensionTypeNameMetadataKey = "ARROW:extension:metadata"
