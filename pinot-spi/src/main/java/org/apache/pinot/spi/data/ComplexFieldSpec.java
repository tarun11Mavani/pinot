/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.StringUtil;


/**
 * FieldSpec for complex fields. The {@link org.apache.pinot.spi.data.FieldSpec.FieldType}
 * is COMPLEX and the inner data type represents the root data type of the field.
 * It could be STRUCT, MAP or LIST. A complex field is composable with a single root type
 * and a number of child types. Although we have multi-value primitive columns, LIST
 * is for representing lists of both complex and primitives inside a complex field.
 *
 * Consider a person json where the root type is STRUCT and composes of inner members:
 *  STRUCT(
 *          name: STRING
 *          age: INT
 *          salary: INT
 *          addresses: LIST (STRUCT
 *                              apt: INT
 *                              street: STRING
 *                              city: STRING
 *                              zip: INT
 *                          )
 *        )
 *
 * The fieldspec would be COMPLEX with type as STRUCT and 4 inner members
 * to model the hierarchy
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ComplexFieldSpec extends FieldSpec {
  public static final String KEY_FIELD = "key";
  public static final String VALUE_FIELD = "value";

  private final Map<String, FieldSpec> _childFieldSpecs;

  @JsonProperty("valueFieldSpecs")
  private Map<String, FieldSpec> _valueFieldSpecs;

  @JsonProperty("defaultValueFieldSpec")
  private FieldSpec _defaultValueFieldSpec;

  // Default constructor required by JSON de-serializer
  public ComplexFieldSpec() {
    super();
    _childFieldSpecs = new HashMap<>();
  }

  public ComplexFieldSpec(String name, DataType dataType, boolean isSingleValueField) {
    super(name, dataType, isSingleValueField);
    Preconditions.checkArgument(dataType == DataType.STRUCT || dataType == DataType.MAP || dataType == DataType.LIST);
    _childFieldSpecs = new HashMap<>();
  }

  public ComplexFieldSpec(String name, DataType dataType, boolean isSingleValueField,
      Map<String, FieldSpec> childFieldSpecs) {
    super(name, dataType, isSingleValueField);
    Preconditions.checkArgument(dataType == DataType.STRUCT || dataType == DataType.MAP || dataType == DataType.LIST);
    _childFieldSpecs = new HashMap<>(childFieldSpecs);
  }

  public static String[] getColumnPath(String column) {
    return column.split("\\$\\$");
  }

  public FieldSpec getChildFieldSpec(String child) {
    return _childFieldSpecs.get(child);
  }

  public Map<String, FieldSpec> getChildFieldSpecs() {
    return _childFieldSpecs;
  }

  @Nullable
  public Map<String, FieldSpec> getValueFieldSpecs() {
    return _valueFieldSpecs;
  }

  public void setValueFieldSpecs(@Nullable Map<String, FieldSpec> valueFieldSpecs) {
    _valueFieldSpecs = valueFieldSpecs;
  }

  @Nullable
  public FieldSpec getDefaultValueFieldSpec() {
    return _defaultValueFieldSpec;
  }

  public void setDefaultValueFieldSpec(@Nullable FieldSpec defaultValueFieldSpec) {
    _defaultValueFieldSpec = defaultValueFieldSpec;
  }

  @JsonIgnore
  @Override
  public FieldType getFieldType() {
    return FieldType.COMPLEX;
  }

  @Override
  public String toString() {
    return "field type: COMPLEX, field name: " + _name + ", root data type: " + _dataType + ", child field specs: "
        + _childFieldSpecs;
  }

  public static class MapFieldSpec {
    private final String _fieldName;
    private final FieldSpec _keyFieldSpec;
    private final FieldSpec _valueFieldSpec;
    private final Map<String, FieldSpec> _valueFieldSpecs;
    private final FieldSpec _defaultValueFieldSpec;

    private MapFieldSpec(ComplexFieldSpec complexFieldSpec) {
      _fieldName = complexFieldSpec.getName();
      Map<String, FieldSpec> children = complexFieldSpec.getChildFieldSpecs();
      if (children.containsKey(KEY_FIELD) && children.containsKey(VALUE_FIELD)) {
        _keyFieldSpec = complexFieldSpec.getChildFieldSpec(KEY_FIELD);
        _valueFieldSpec = complexFieldSpec.getChildFieldSpec(VALUE_FIELD);
      } else {
        _keyFieldSpec = new DimensionFieldSpec(KEY_FIELD, DataType.STRING, true);
        _valueFieldSpec = new DimensionFieldSpec(VALUE_FIELD, DataType.STRING, true);
      }
      _valueFieldSpecs = complexFieldSpec.getValueFieldSpecs();
      _defaultValueFieldSpec = complexFieldSpec.getDefaultValueFieldSpec();
    }

    public String getFieldName() {
      return _fieldName;
    }

    public FieldSpec getKeyFieldSpec() {
      return _keyFieldSpec;
    }

    public FieldSpec getValueFieldSpec() {
      return _valueFieldSpec;
    }

    @Nullable
    public Map<String, FieldSpec> getValueFieldSpecs() {
      return _valueFieldSpecs;
    }

    @Nullable
    public FieldSpec getDefaultValueFieldSpec() {
      return _defaultValueFieldSpec;
    }
  }

  public static MapFieldSpec toMapFieldSpec(ComplexFieldSpec complexFieldSpec) {
    return new MapFieldSpec(complexFieldSpec);
  }

  public static ComplexFieldSpec fromMapFieldSpec(MapFieldSpec mapFieldSpec) {
    return new ComplexFieldSpec(mapFieldSpec.getFieldName(), DataType.MAP, true,
        Map.of(KEY_FIELD, mapFieldSpec.getKeyFieldSpec(), VALUE_FIELD, mapFieldSpec.getValueFieldSpec()));
  }

  /**
   * Returns the full child name for the given columns for complex data type.
   * E.g. map$$key, map$$value, list$$element, etc.
   * This is used in persisting column metadata for complex data types.
   */
  public static String getFullChildName(String... columns) {
    return StringUtil.join("$$", columns);
  }

  public ObjectNode toJsonObject() {
    ObjectNode jsonObject = super.toJsonObject();
    // Always emit childFieldSpecs for MAP to preserve wire compatibility with older brokers/servers
    // that deserialize MAP columns via ComplexFieldSpec.toMapFieldSpec() and expect key/value children.
    // For MAP columns without explicit children, emit the legacy STRING defaults.
    Map<String, FieldSpec> childSpecs = _childFieldSpecs;
    if (childSpecs.isEmpty() && _dataType == DataType.MAP) {
      childSpecs = Map.of(
          KEY_FIELD, new DimensionFieldSpec(KEY_FIELD, DataType.STRING, true),
          VALUE_FIELD, new DimensionFieldSpec(VALUE_FIELD, DataType.STRING, true));
    }
    if (!childSpecs.isEmpty()) {
      ObjectNode childFieldSpecsNode = JsonUtils.newObjectNode();
      for (Map.Entry<String, FieldSpec> entry : childSpecs.entrySet()) {
        childFieldSpecsNode.put(entry.getKey(), entry.getValue().toJsonObject());
      }
      jsonObject.put("childFieldSpecs", childFieldSpecsNode);
    }
    if (_valueFieldSpecs != null && !_valueFieldSpecs.isEmpty()) {
      ObjectNode valueFieldSpecsNode = JsonUtils.newObjectNode();
      for (Map.Entry<String, FieldSpec> entry : _valueFieldSpecs.entrySet()) {
        valueFieldSpecsNode.set(entry.getKey(), entry.getValue().toJsonObject());
      }
      jsonObject.set("valueFieldSpecs", valueFieldSpecsNode);
    }
    if (_defaultValueFieldSpec != null) {
      jsonObject.set("defaultValueFieldSpec", _defaultValueFieldSpec.toJsonObject());
    }
    return jsonObject;
  }
}
