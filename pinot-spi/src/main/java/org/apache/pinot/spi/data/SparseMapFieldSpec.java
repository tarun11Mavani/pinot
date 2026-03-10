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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * FieldSpec for SPARSE_MAP columns. Each SPARSE_MAP column stores a per-document sparse key→value
 * mapping with a declared set of key types and an optional catch-all default value type.
 *
 * <p>Analogous to {@link ComplexFieldSpec} for complex (nested) columns, this class gives SPARSE_MAP
 * its own {@link FieldType} and a dedicated JSON representation under {@code sparseMapFieldSpecs}.
 *
 * <p>Thread-safety: instances are mutable (setters are required by Jackson); callers should not
 * share instances across threads without external synchronization.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class SparseMapFieldSpec extends FieldSpec {

  private Map<String, DataType> _keyTypes;
  private DataType _defaultValueType = DataType.STRING;

  // Default constructor required by JSON de-serializer. DO NOT REMOVE.
  public SparseMapFieldSpec() {
    super();
    _dataType = DataType.SPARSE_MAP;
    _defaultNullValue = getDefaultNullValue(FieldType.SPARSE_MAP, DataType.SPARSE_MAP, null);
  }

  public SparseMapFieldSpec(String name) {
    super(name, DataType.SPARSE_MAP, true);
  }

  public SparseMapFieldSpec(String name, Map<String, DataType> keyTypes) {
    super(name, DataType.SPARSE_MAP, true);
    _keyTypes = keyTypes;
  }

  public SparseMapFieldSpec(String name, Map<String, DataType> keyTypes, DataType defaultValueType) {
    super(name, DataType.SPARSE_MAP, true);
    _keyTypes = keyTypes;
    _defaultValueType = defaultValueType;
  }

  @Nullable
  public Map<String, DataType> getKeyTypes() {
    return _keyTypes;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setKeyTypes(@Nullable Map<String, DataType> keyTypes) {
    _keyTypes = keyTypes;
  }

  public DataType getDefaultValueType() {
    return _defaultValueType;
  }

  // Required by JSON de-serializer. DO NOT REMOVE.
  public void setDefaultValueType(DataType defaultValueType) {
    _defaultValueType = defaultValueType;
  }

  @JsonIgnore
  @Override
  public FieldType getFieldType() {
    return FieldType.SPARSE_MAP;
  }

  @Override
  public ObjectNode toJsonObject() {
    ObjectNode jsonObject = super.toJsonObject();
    if (_keyTypes != null && !_keyTypes.isEmpty()) {
      ObjectNode keyTypesNode = JsonUtils.newObjectNode();
      for (Map.Entry<String, DataType> entry : _keyTypes.entrySet()) {
        keyTypesNode.put(entry.getKey(), entry.getValue().name());
      }
      jsonObject.set("keyTypes", keyTypesNode);
    }
    if (_defaultValueType != null && _defaultValueType != DataType.STRING) {
      jsonObject.put("defaultValueType", _defaultValueType.name());
    }
    return jsonObject;
  }

  @Override
  public String toString() {
    return "< field type: SPARSE_MAP, field name: " + _name + ", key types: " + _keyTypes
        + ", default value type: " + _defaultValueType + " >";
  }
}
