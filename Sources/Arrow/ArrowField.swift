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

/// Describes a single column in a [`Schema`](super::Schema).
///
/// A [`Schema`](super::Schema) is an ordered collection of
/// [`Field`] objects. Fields contain:
/// * `name`: the name of the field
/// * `data_type`: the type of the field
/// * `nullable`: if the field is nullable
/// * `metadata`: a map of key-value pairs containing additional custom metadata
///
/// Arrow Extension types, are encoded in `Field`s metadata. See
/// [`Self::try_extension_type`] to retrieve the [`ExtensionType`], if any.
public struct ArrowField: Codable, Sendable {
  public var name: String
  public var dataType: ArrowType
  /// Indicates whether this [`Field`] supports null values.
  ///
  /// If true, the field *may* contain null values.
  public var nullable: Bool
  public let dictIsOrdered: Bool
  /// A map of key-value pairs containing additional custom meta data.
  public var metadata: [String: String]
}

// Auto-derive `PartialEq` traits will pull `dict_id` and `dict_is_ordered`
// into comparison. However, these properties are only used in IPC context
// for matching dictionary encoded data. They are not necessary to be same
// to consider schema equality. For example, in C++ `Field` implementation,
// it doesn't contain these dictionary properties too.

extension ArrowField: Equatable {

  public static func == (lhs: ArrowField, rhs: ArrowField) -> Bool {
    lhs.name == rhs.name
      && lhs.dataType == rhs.dataType
      && lhs.nullable == rhs.nullable
      && lhs.metadata == rhs.metadata
  }
}

extension ArrowField {
  /// Default list member field name.
  public static let listFieldDefaultName = "item"

  /// Creates a new field with the given name, data type, and nullability.
  public init(name: String, dataType: ArrowType, nullable: Bool) {
    self.name = name
    self.dataType = dataType
    self.nullable = nullable
    self.dictIsOrdered = false
    self.metadata = .init()
  }

  /// Creates a new `ArrowFieldField` suitable for `ArrowType::List`.
  ///
  /// While not required, this method follows the convention of naming the
  /// `Field` `"item"`.
  public init(listFieldWith dataType: ArrowType, nullable: Bool) {
    self.init(
      name: Self.listFieldDefaultName,
      dataType: dataType,
      nullable: nullable
    )
  }

  /// Create a new `ArrowField` suitable for `ArrowType::Dictionary`
  ///
  public init(
    dictWithName: String,
    key: ArrowType,
    value: ArrowType,
    nullable: Bool
  ) {
    precondition(
      key.isDictionaryKeyType,
      "\(key) is not a valid dictionary key"
    )
    let dataType: ArrowType = .dictionary(key, value)
    self = Self(name: dictWithName, dataType: dataType, nullable: nullable)
  }

  /// Create a new [`Field`] with [`DataType::Struct`]
  ///
  /// - `name`: the name of the [`DataType::Struct`] field
  /// - `fields`: the description of each struct element
  /// - `nullable`: if the [`DataType::Struct`] array is nullable
  public init(structWithName name: String, fields: Fields, nullable: Bool) {
    self.init(name: name, dataType: .strct(fields), nullable: nullable)
  }

  /// Create a new [`Field`] with [`DataType::List`]
  ///
  /// - `name`: the name of the [`DataType::List`] field
  /// - `value`: the description of each list element
  /// - `nullable`: if the [`DataType::List`] array is nullable
  public init(list name: String, value: ArrowField, nullable: Bool) {
    self.init(name: name, dataType: .list(value), nullable: nullable)
  }

  /// Create a new `ArrowField` with `ArrowType::LargeList`.
  ///
  /// - Parameters:
  ///   - name: The name of the field.
  ///   - value: the description of each list element.
  ///   - nullable: true if the field is nullable.
  public init(
    largeListNamed name: String,
    value: ArrowField,
    nullable: Bool,
  ) {
    self = Self(
      name: name, dataType: .largeList(value), nullable: nullable)
  }

  /// Create a new `ArrowField` with `ArrowType.FixedSizeList`.
  ///
  /// - Parameters:
  ///   - name: The name of the field.
  ///   - value: the description of each list element.
  ///   - size: the list size
  ///   - nullable: true if the field is nullable.
  public init(
    fixedSizeListNamed name: String,
    value: ArrowField,
    size: Int32,
    nullable: Bool,
  ) {
    self.init(
      name: name,
      dataType: .fixedSizeList(value, size),
      nullable: nullable
    )
  }

  /// Sets the `ArrowField`'s optional custom metadata.
  @inlinable
  public mutating func setMetadata(metadata: [String: String]) {
    self.metadata = metadata
  }

  /// Sets the metadata of this `ArrowField` to be `metadata` and returns self.
  public mutating func withMetadata(metadata: [String: String]) -> Self {
    self.setMetadata(metadata: metadata)
    return self
  }

  /// Set the name of this `ArrowField`.
  @inlinable
  public mutating func set_name(name: String) {
    self.name = name
  }

  /// Set the name of the `ArrowField` and returns self.
  public mutating func withName(name: String) -> Self {
    self.name = name
    return self
  }

  /// Set [`DataType`] of the [`Field`]
  ///
  /// ```
  /// # use arrow_schema::*;
  /// let mut field = Field::new("c1", DataType::Int64, false);
  /// field.set_data_type(DataType::Utf8);
  ///
  /// assert_eq!(field.data_type(), &DataType::Utf8);
  /// ```
  @inlinable
  public mutating func set_data_type(dataType: ArrowType) {
    self.dataType = dataType
  }

  /// Set [`DataType`] of the [`Field`] and returns self.
  ///
  /// ```
  /// # use arrow_schema::*;
  /// let field = Field::new("c1", DataType::Int64, false)
  ///    .with_data_type(DataType::Utf8);
  ///
  /// assert_eq!(field.data_type(), &DataType::Utf8);
  /// ```
  public mutating func withDataType(_ data_type: ArrowType) -> Self {
    self.dataType = data_type
    return self
  }

  /// Returns the extension type name of this [`Field`], if set.
  ///
  /// This returns the value of [`EXTENSION_TYPE_NAME_KEY`], if set in
  /// [`Field::metadata`]. If the key is missing, there is no extension type
  /// name and this returns `None`.
  ///
  /// # Example
  ///
  /// ```
  /// # use arrow_schema::{DataType, extension::EXTENSION_TYPE_NAME_KEY, Field};
  ///
  /// let field = Field::new("", DataType::Null, false);
  /// assert_eq!(field.extension_type_name(), None);
  ///
  /// let field = Field::new("", DataType::Null, false).with_metadata(
  ///    [(EXTENSION_TYPE_NAME_KEY.to_owned(), "example".to_owned())]
  ///        .into_iter()
  ///        .collect(),
  /// );
  /// assert_eq!(field.extension_type_name(), Some("example"));
  /// ```
  public var extension_type_name: String? {
    self.metadata[extensionTypeNameKey]
  }

  /// Returns the extension type metadata of this [`Field`], if set.
  ///
  /// This returns the value of [`EXTENSION_TYPE_METADATA_KEY`], if set in
  /// [`Field::metadata`]. If the key is missing, there is no extension type
  /// metadata and this returns `None`.
  ///
  /// # Example
  ///
  /// ```
  /// # use arrow_schema::{DataType, extension::EXTENSION_TYPE_METADATA_KEY, Field};
  ///
  /// let field = Field::new("", DataType::Null, false);
  /// assert_eq!(field.extension_type_metadata(), None);
  ///
  /// let field = Field::new("", .Null, false).with_metadata(
  ///    [(EXTENSION_TYPE_METADATA_KEY.to_owned(), "example".to_owned())]
  ///        .into_iter()
  ///        .collect(),
  /// );
  /// assert_eq!(field.extension_type_metadata(), Some("example"));
  /// ```
  public var extension_type_metadata: String? {
    self.metadata[extensionTypeNameMetadataKey]
  }

  @inlinable
  var is_nullable: Bool {
    self.nullable
  }

  /// Set the `nullable` of this [`Field`].
  ///
  /// ```
  /// # use arrow_schema::*;
  /// let mut field = Field::new("c1", .Int64, false);
  /// field.set_nullable(true);
  ///
  /// assert_eq!(field.is_nullable(), true);
  /// ```
  @inlinable
  public mutating func setNullable(nullable: Bool) {
    self.nullable = nullable
  }

  /// Set `nullable` of the [`Field`] and returns self.
  ///
  /// ```
  /// # use arrow_schema::*;
  /// let field = Field::new("c1", .Int64, false)
  ///    .with_nullable(true);
  ///
  /// assert_eq!(field.is_nullable(), true);
  /// ```
  public mutating func with_nullable(nullable: Bool) -> Self {
    self.nullable = nullable
    return self
  }

  /// Returns whether this `Field`'s dictionary is ordered, if this is a dictionary type.
  @inlinable
  public var dict_is_ordered: Bool {

    switch self.dataType {
    case .dictionary: return self.dictIsOrdered
    default: return false
    }
  }

  /// Set the is ordered field for this `Field`, if it is a dictionary.
  ///
  /// Does nothing if this is not a dictionary type.
  ///
  /// See [`Field::dict_is_ordered`] for more information.
  //    pub fn with_dict_is_ordered(mut self, dict_is_ordered: Bool) -> Self {
  //        if matches!(self.data_type, .Dictionary(_, _)) {
  //            self.dict_is_ordered = dict_is_ordered;
  //        };
  //        self
  //    }

  /// Merge this field into self if it is compatible.
  ///
  /// Struct fields are merged recursively.
  ///
  /// NOTE: `self` may be updated to a partial / unexpected state in case of merge failure.
  ///
  /// Example:
  ///
  /// ```
  /// # use arrow_schema::*;
  /// let mut field = Field::new("c1", .Int64, false);
  /// assert!(field.try_merge(&Field::new("c1", .Int64, true)).is_ok());
  /// assert!(field.is_nullable());
  /// ```
  //    pub fn try_merge(&mut self, from: &Field) -> Result<(), ArrowError> {
  //        if from.dict_is_ordered != self.dict_is_ordered {
  //            return Err(ArrowError::SchemaError(format!(
  //                "Fail to merge schema field '{}' because from dict_is_ordered = {} does not match {}",
  //                self.name, from.dict_is_ordered, self.dict_is_ordered
  //            )));
  //        }
  //        // merge metadata
  //        match (self.metadata().is_empty(), from.metadata().is_empty()) {
  //            (false, false) => {
  //                let mut merged = self.metadata().clone();
  //                for (key, from_value) in from.metadata() {
  //                    if let Some(self_value) = self.metadata.get(key) {
  //                        if self_value != from_value {
  //                            return Err(ArrowError::SchemaError(format!(
  //                                "Fail to merge field '{}' due to conflicting metadata data value for key {}.
  //                                    From value = {} does not match {}", self.name, key, from_value, self_value),
  //                            ));
  //                        }
  //                    } else {
  //                        merged.insert(key.clone(), from_value.clone());
  //                    }
  //                }
  //                self.set_metadata(merged);
  //            }
  //            (true, false) => {
  //                self.set_metadata(from.metadata().clone());
  //            }
  //            _ => {}
  //        }
  //        match &mut self.data_type {
  //            DataType::Struct(nested_fields) => match &from.data_type {
  //                DataType::Struct(from_nested_fields) => {
  //                    let mut builder = SchemaBuilder::new();
  //                    nested_fields.iter().chain(from_nested_fields).try_for_each(|f| builder.try_merge(f))?;
  //                    *nested_fields = builder.finish().fields;
  //                }
  //                _ => {
  //                    return Err(ArrowError::SchemaError(
  //                        format!("Fail to merge schema field '{}' because the from data_type = {} is not DataType::Struct",
  //                            self.name, from.data_type)
  //                ))}
  //            },
  //            DataType::Union(nested_fields, _) => match &from.data_type {
  //                DataType::Union(from_nested_fields, _) => {
  //                    nested_fields.try_merge(from_nested_fields)?
  //                }
  //                _ => {
  //                    return Err(ArrowError::SchemaError(
  //                        format!("Fail to merge schema field '{}' because the from data_type = {} is not DataType::Union",
  //                            self.name, from.data_type)
  //                    ));
  //                }
  //            },
  //            DataType::List(field) => match &from.data_type {
  //                DataType::List(from_field) => {
  //                    let mut f = (**field).clone();
  //                    f.try_merge(from_field)?;
  //                    (*field) = Arc::new(f);
  //                },
  //                _ => {
  //                    return Err(ArrowError::SchemaError(
  //                        format!("Fail to merge schema field '{}' because the from data_type = {} is not DataType::List",
  //                            self.name, from.data_type)
  //                ))}
  //            },
  //            DataType::LargeList(field) => match &from.data_type {
  //                DataType::LargeList(from_field) => {
  //                    let mut f = (**field).clone();
  //                    f.try_merge(from_field)?;
  //                    (*field) = Arc::new(f);
  //                },
  //                _ => {
  //                    return Err(ArrowError::SchemaError(
  //                        format!("Fail to merge schema field '{}' because the from data_type = {} is not DataType::LargeList",
  //                            self.name, from.data_type)
  //                ))}
  //            },
  //            DataType::Null => {
  //                self.nullable = true;
  //                self.data_type = from.data_type.clone();
  //            }
  //            | DataType::Boolean
  //            | DataType::Int8
  //            | DataType::Int16
  //            | DataType::Int32
  //            | DataType::Int64
  //            | DataType::UInt8
  //            | DataType::UInt16
  //            | DataType::UInt32
  //            | DataType::UInt64
  //            | DataType::Float16
  //            | DataType::Float32
  //            | DataType::Float64
  //            | DataType::Timestamp(_, _)
  //            | DataType::Date32
  //            | DataType::Date64
  //            | DataType::Time32(_)
  //            | DataType::Time64(_)
  //            | DataType::Duration(_)
  //            | DataType::Binary
  //            | DataType::LargeBinary
  //            | DataType::BinaryView
  //            | DataType::Interval(_)
  //            | DataType::LargeListView(_)
  //            | DataType::ListView(_)
  //            | DataType::Map(_, _)
  //            | DataType::Dictionary(_, _)
  //            | DataType::RunEndEncoded(_, _)
  //            | DataType::FixedSizeList(_, _)
  //            | DataType::FixedSizeBinary(_)
  //            | DataType::Utf8
  //            | DataType::LargeUtf8
  //            | DataType::Utf8View
  //            | DataType::Decimal32(_, _)
  //            | DataType::Decimal64(_, _)
  //            | DataType::Decimal128(_, _)
  //            | DataType::Decimal256(_, _) => {
  //                if from.data_type == DataType::Null {
  //                    self.nullable = true;
  //                } else if self.data_type != from.data_type {
  //                    return Err(ArrowError::SchemaError(
  //                        format!("Fail to merge schema field '{}' because the from data_type = {} does not equal {}",
  //                            self.name, from.data_type, self.data_type)
  //                    ));
  //                }
  //            }
  //        }
  //        self.nullable |= from.nullable;
  //
  //        Ok(())
  //    }

  /// Check to see if `self` is a superset of `other` field. Superset is defined as:
  ///
  /// * if nullability doesn't match, self needs to be nullable
  /// * self.metadata is a superset of other.metadata
  /// * all other fields are equal
  public func contains(other: ArrowField) -> Bool {
    self.name == other.name
      && self.dataType.contains(other.dataType)
      && self.dict_is_ordered == other.dict_is_ordered
      // self need to be nullable or both of them are not nullable
      && (self.nullable || !other.nullable)
      // make sure self.metadata is a superset of other.metadata
      && other.metadata.allSatisfy { (key, v1) in
        self.metadata[key].map { v2 in v1 == v2 } ?? false
      }
  }

}

// TODO: improve display with crate https://crates.io/crates/derive_more ?
//impl std::fmt::Display for Field {
//    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//        write!(f, "{self:?}")
//    }
//}

//#[cfg(test)]
//mod test {
//    use super::*;
//    use std::collections::hash_map::DefaultHasher;
//
//    #[test]
//    fn test_new_with_string() {
//        // Fields should allow owned Strings to support reuse
//        let s = "c1";
//        Field::new(s, DataType::Int64, false);
//    }
//
//    #[test]
//    fn test_new_dict_with_string() {
//        // Fields should allow owned Strings to support reuse
//        let s = "c1";
//        #[allow(deprecated)]
//        Field::new_dict(s, DataType::Int64, false, 4, false);
//    }
//
//    #[test]
//    fn test_merge_incompatible_types() {
//        let mut field = Field::new("c1", DataType::Int64, false);
//        let result = field
//            .try_merge(&Field::new("c1", DataType::Float32, true))
//            .expect_err("should fail")
//            .to_string();
//        assert_eq!("Schema error: Fail to merge schema field 'c1' because the from data_type = Float32 does not equal Int64", result);
//    }
//
//    #[test]
//    fn test_merge_with_null() {
//        let mut field1 = Field::new("c1", DataType::Null, true);
//        field1
//            .try_merge(&Field::new("c1", DataType::Float32, false))
//            .expect("should widen type to nullable float");
//        assert_eq!(Field::new("c1", DataType::Float32, true), field1);
//
//        let mut field2 = Field::new("c2", DataType::Utf8, false);
//        field2
//            .try_merge(&Field::new("c2", DataType::Null, true))
//            .expect("should widen type to nullable utf8");
//        assert_eq!(Field::new("c2", DataType::Utf8, true), field2);
//    }
//
//    #[test]
//    fn test_merge_with_nested_null() {
//        let mut struct1 = Field::new(
//            "s1",
//            DataType::Struct(Fields::from(vec![Field::new(
//                "inner",
//                DataType::Float32,
//                false,
//            )])),
//            false,
//        );
//
//        let struct2 = Field::new(
//            "s2",
//            DataType::Struct(Fields::from(vec![Field::new(
//                "inner",
//                DataType::Null,
//                false,
//            )])),
//            true,
//        );
//
//        struct1
//            .try_merge(&struct2)
//            .expect("should widen inner field's type to nullable float");
//        assert_eq!(
//            Field::new(
//                "s1",
//                DataType::Struct(Fields::from(vec![Field::new(
//                    "inner",
//                    DataType::Float32,
//                    true,
//                )])),
//                true,
//            ),
//            struct1
//        );
//
//        let mut list1 = Field::new(
//            "l1",
//            DataType::List(Field::new("inner", DataType::Float32, false).into()),
//            false,
//        );
//
//        let list2 = Field::new(
//            "l2",
//            DataType::List(Field::new("inner", DataType::Null, false).into()),
//            true,
//        );
//
//        list1
//            .try_merge(&list2)
//            .expect("should widen inner field's type to nullable float");
//        assert_eq!(
//            Field::new(
//                "l1",
//                DataType::List(Field::new("inner", DataType::Float32, true).into()),
//                true,
//            ),
//            list1
//        );
//
//        let mut large_list1 = Field::new(
//            "ll1",
//            DataType::LargeList(Field::new("inner", DataType::Float32, false).into()),
//            false,
//        );
//
//        let large_list2 = Field::new(
//            "ll2",
//            DataType::LargeList(Field::new("inner", DataType::Null, false).into()),
//            true,
//        );
//
//        large_list1
//            .try_merge(&large_list2)
//            .expect("should widen inner field's type to nullable float");
//        assert_eq!(
//            Field::new(
//                "ll1",
//                DataType::LargeList(Field::new("inner", DataType::Float32, true).into()),
//                true,
//            ),
//            large_list1
//        );
//    }
//
//    #[test]
//    fn test_fields_with_dict_id() {
//        #[allow(deprecated)]
//        let dict1 = Field::new_dict(
//            "dict1",
//            DataType::Dictionary(DataType::Utf8.into(), DataType::Int32.into()),
//            false,
//            10,
//            false,
//        );
//        #[allow(deprecated)]
//        let dict2 = Field::new_dict(
//            "dict2",
//            DataType::Dictionary(DataType::Int32.into(), DataType::Int8.into()),
//            false,
//            20,
//            false,
//        );
//
//        let field = Field::new(
//            "struct<dict1, list[struct<dict2, list[struct<dict1]>]>",
//            DataType::Struct(Fields::from(vec![
//                dict1.clone(),
//                Field::new(
//                    "list[struct<dict1, list[struct<dict2>]>]",
//                    DataType::List(Arc::new(Field::new(
//                        "struct<dict1, list[struct<dict2>]>",
//                        DataType::Struct(Fields::from(vec![
//                            dict1.clone(),
//                            Field::new(
//                                "list[struct<dict2>]",
//                                DataType::List(Arc::new(Field::new(
//                                    "struct<dict2>",
//                                    DataType::Struct(vec![dict2.clone()].into()),
//                                    false,
//                                ))),
//                                false,
//                            ),
//                        ])),
//                        false,
//                    ))),
//                    false,
//                ),
//            ])),
//            false,
//        );
//
//        #[allow(deprecated)]
//        for field in field.fields_with_dict_id(10) {
//            assert_eq!(dict1, *field);
//        }
//        #[allow(deprecated)]
//        for field in field.fields_with_dict_id(20) {
//            assert_eq!(dict2, *field);
//        }
//    }
//
//    fn get_field_hash(field: &Field) -> u64 {
//        let mut s = DefaultHasher::new();
//        field.hash(&mut s);
//        s.finish()
//    }
//
//    #[test]
//    fn test_field_comparison_case() {
//        // dictionary-encoding properties not used for field comparison
//        #[allow(deprecated)]
//        let dict1 = Field::new_dict(
//            "dict1",
//            DataType::Dictionary(DataType::Utf8.into(), DataType::Int32.into()),
//            false,
//            10,
//            false,
//        );
//        #[allow(deprecated)]
//        let dict2 = Field::new_dict(
//            "dict1",
//            DataType::Dictionary(DataType::Utf8.into(), DataType::Int32.into()),
//            false,
//            20,
//            false,
//        );
//
//        assert_eq!(dict1, dict2);
//        assert_eq!(get_field_hash(&dict1), get_field_hash(&dict2));
//
//        #[allow(deprecated)]
//        let dict1 = Field::new_dict(
//            "dict0",
//            DataType::Dictionary(DataType::Utf8.into(), DataType::Int32.into()),
//            false,
//            10,
//            false,
//        );
//
//        assert_ne!(dict1, dict2);
//        assert_ne!(get_field_hash(&dict1), get_field_hash(&dict2));
//    }
//
//    #[test]
//    fn test_field_comparison_metadata() {
//        let f1 = Field::new("x", DataType::Binary, false).with_metadata(HashMap::from([
//            (String::from("k1"), String::from("v1")),
//            (String::from("k2"), String::from("v2")),
//        ]));
//        let f2 = Field::new("x", DataType::Binary, false).with_metadata(HashMap::from([
//            (String::from("k1"), String::from("v1")),
//            (String::from("k3"), String::from("v3")),
//        ]));
//        let f3 = Field::new("x", DataType::Binary, false).with_metadata(HashMap::from([
//            (String::from("k1"), String::from("v1")),
//            (String::from("k3"), String::from("v4")),
//        ]));
//
//        assert!(f1.cmp(&f2).is_lt());
//        assert!(f2.cmp(&f3).is_lt());
//        assert!(f1.cmp(&f3).is_lt());
//    }
//
//    #[test]
//    fn test_contains_reflexivity() {
//        let mut field = Field::new("field1", DataType::Float16, false);
//        field.set_metadata(HashMap::from([
//            (String::from("k0"), String::from("v0")),
//            (String::from("k1"), String::from("v1")),
//        ]));
//        assert!(field.contains(&field))
//    }
//
//    #[test]
//    fn test_contains_transitivity() {
//        let child_field = Field::new("child1", DataType::Float16, false);
//
//        let mut field1 = Field::new(
//            "field1",
//            DataType::Struct(Fields::from(vec![child_field])),
//            false,
//        );
//        field1.set_metadata(HashMap::from([(String::from("k1"), String::from("v1"))]));
//
//        let mut field2 = Field::new("field1", DataType::Struct(Fields::default()), true);
//        field2.set_metadata(HashMap::from([(String::from("k2"), String::from("v2"))]));
//        field2.try_merge(&field1).unwrap();
//
//        let mut field3 = Field::new("field1", DataType::Struct(Fields::default()), false);
//        field3.set_metadata(HashMap::from([(String::from("k3"), String::from("v3"))]));
//        field3.try_merge(&field2).unwrap();
//
//        assert!(field2.contains(&field1));
//        assert!(field3.contains(&field2));
//        assert!(field3.contains(&field1));
//
//        assert!(!field1.contains(&field2));
//        assert!(!field1.contains(&field3));
//        assert!(!field2.contains(&field3));
//    }
//
//    #[test]
//    fn test_contains_nullable() {
//        let field1 = Field::new("field1", DataType::Boolean, true);
//        let field2 = Field::new("field1", DataType::Boolean, false);
//        assert!(field1.contains(&field2));
//        assert!(!field2.contains(&field1));
//    }
//
//    #[test]
//    fn test_contains_must_have_same_fields() {
//        let child_field1 = Field::new("child1", DataType::Float16, false);
//        let child_field2 = Field::new("child2", DataType::Float16, false);
//
//        let field1 = Field::new(
//            "field1",
//            DataType::Struct(vec![child_field1.clone()].into()),
//            true,
//        );
//        let field2 = Field::new(
//            "field1",
//            DataType::Struct(vec![child_field1, child_field2].into()),
//            true,
//        );
//
//        assert!(!field1.contains(&field2));
//        assert!(!field2.contains(&field1));
//
//        // UnionFields with different type ID
//        let field1 = Field::new(
//            "field1",
//            DataType::Union(
//                UnionFields::new(
//                    vec![1, 2],
//                    vec![
//                        Field::new("field1", DataType::UInt8, true),
//                        Field::new("field3", DataType::Utf8, false),
//                    ],
//                ),
//                UnionMode::Dense,
//            ),
//            true,
//        );
//        let field2 = Field::new(
//            "field1",
//            DataType::Union(
//                UnionFields::new(
//                    vec![1, 3],
//                    vec![
//                        Field::new("field1", DataType::UInt8, false),
//                        Field::new("field3", DataType::Utf8, false),
//                    ],
//                ),
//                UnionMode::Dense,
//            ),
//            true,
//        );
//        assert!(!field1.contains(&field2));
//
//        // UnionFields with same type ID
//        let field1 = Field::new(
//            "field1",
//            DataType::Union(
//                UnionFields::new(
//                    vec![1, 2],
//                    vec![
//                        Field::new("field1", DataType::UInt8, true),
//                        Field::new("field3", DataType::Utf8, false),
//                    ],
//                ),
//                UnionMode::Dense,
//            ),
//            true,
//        );
//        let field2 = Field::new(
//            "field1",
//            DataType::Union(
//                UnionFields::new(
//                    vec![1, 2],
//                    vec![
//                        Field::new("field1", DataType::UInt8, false),
//                        Field::new("field3", DataType::Utf8, false),
//                    ],
//                ),
//                UnionMode::Dense,
//            ),
//            true,
//        );
//        assert!(field1.contains(&field2));
//    }
//
//    #[cfg(feature = "serde")]
//    fn assert_binary_serde_round_trip(field: Field) {
//        let serialized = bincode::serialize(&field).unwrap();
//        let deserialized: Field = bincode::deserialize(&serialized).unwrap();
//        assert_eq!(field, deserialized)
//    }
//
//    #[cfg(feature = "serde")]
//    #[test]
//    fn test_field_without_metadata_serde() {
//        let field = Field::new("name", DataType::Boolean, true);
//        assert_binary_serde_round_trip(field)
//    }
//
//    #[cfg(feature = "serde")]
//    #[test]
//    fn test_field_with_empty_metadata_serde() {
//        let field = Field::new("name", DataType::Boolean, false).with_metadata(HashMap::new());
//
//        assert_binary_serde_round_trip(field)
//    }
//
//    #[cfg(feature = "serde")]
//    #[test]
//    fn test_field_with_nonempty_metadata_serde() {
//        let mut metadata = HashMap::new();
//        metadata.insert("hi".to_owned(), "".to_owned());
//        let field = Field::new("name", DataType::Boolean, false).with_metadata(metadata);
//
//        assert_binary_serde_round_trip(field)
//    }
//}
