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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Unit tests for SPARSE_MAP DataType and Schema integration using SparseMapFieldSpec.
 */
public class SparseMapDataTypeTest {

  // ---- DataType tests ----

  @Test
  public void testSparseMapDataTypeProperties() {
    FieldSpec.DataType dt = FieldSpec.DataType.SPARSE_MAP;
    // SPARSE_MAP is stored as itself
    assertEquals(dt.getStoredType(), FieldSpec.DataType.SPARSE_MAP);
    // SPARSE_MAP does not have a fixed width
    assertFalse(dt.isFixedWidth());
    // SPARSE_MAP is not numeric
    assertFalse(dt.isNumeric());
  }

  @Test
  public void testSparseMapNullValuePlaceholder() {
    Object placeholder = CommonConstants.NullValuePlaceHolder.SPARSE_MAP;
    assertNotNull(placeholder);
    assertTrue(placeholder instanceof Map);
    assertTrue(((Map<?, ?>) placeholder).isEmpty());
  }

  // ---- SparseMapFieldSpec tests ----

  @Test
  public void testSparseMapFieldSpecKeyTypes() {
    SparseMapFieldSpec spec = new SparseMapFieldSpec("col");

    // Initially null
    assertNull(spec.getKeyTypes());

    // Default value type is STRING
    assertEquals(spec.getDefaultValueType(), FieldSpec.DataType.STRING);

    // Set explicit key types
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("age", FieldSpec.DataType.INT);
    keyTypes.put("name", FieldSpec.DataType.STRING);
    spec.setKeyTypes(keyTypes);
    spec.setDefaultValueType(FieldSpec.DataType.DOUBLE);

    assertEquals(spec.getKeyTypes().get("age"), FieldSpec.DataType.INT);
    assertEquals(spec.getKeyTypes().get("name"), FieldSpec.DataType.STRING);
    assertEquals(spec.getDefaultValueType(), FieldSpec.DataType.DOUBLE);
  }

  @Test
  public void testSparseMapFieldSpecFieldType() {
    SparseMapFieldSpec spec = new SparseMapFieldSpec("col");
    assertEquals(spec.getFieldType(), FieldSpec.FieldType.SPARSE_MAP);
    assertEquals(spec.getDataType(), FieldSpec.DataType.SPARSE_MAP);
  }

  // ---- Schema validation tests ----

  @Test
  public void testSchemaValidationAcceptsSparseMapField() {
    Schema schema = new Schema();
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("clicks", FieldSpec.DataType.LONG);
    keyTypes.put("tag", FieldSpec.DataType.STRING);
    SparseMapFieldSpec spec = new SparseMapFieldSpec("features", keyTypes);
    schema.addField(spec);
    schema.validate(); // Should not throw
    assertTrue(schema.hasSparseMapColumn());
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testSchemaValidationRejectsSparseMapAsMetric() {
    Schema schema = new Schema();
    MetricFieldSpec spec = new MetricFieldSpec("features", FieldSpec.DataType.SPARSE_MAP);
    schema.addField(spec);
    schema.validate(); // Should throw because SPARSE_MAP not allowed as METRIC
  }

  @Test
  public void testSchemaHasSparseMapColumnFlagFalseWhenNone() {
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec("d", FieldSpec.DataType.STRING, true));
    schema.addField(new MetricFieldSpec("m", FieldSpec.DataType.LONG));
    assertFalse(schema.hasSparseMapColumn());
  }

  @Test
  public void testSchemaMultipleSparseMapColumns() {
    Schema schema = new Schema();

    SparseMapFieldSpec features = new SparseMapFieldSpec("features",
        Collections.singletonMap("clicks", FieldSpec.DataType.LONG));
    schema.addField(features);

    SparseMapFieldSpec labels = new SparseMapFieldSpec("labels",
        Collections.singletonMap("label", FieldSpec.DataType.STRING));
    schema.addField(labels);

    schema.addField(new DimensionFieldSpec("userId", FieldSpec.DataType.STRING, true));
    schema.validate();
    assertTrue(schema.hasSparseMapColumn());
    assertNotNull(schema.getFieldSpecFor("features"));
    assertNotNull(schema.getFieldSpecFor("labels"));
    assertEquals(schema.getFieldSpecFor("features").getDataType(), FieldSpec.DataType.SPARSE_MAP);
    assertEquals(schema.getFieldSpecFor("features").getFieldType(), FieldSpec.FieldType.SPARSE_MAP);
  }

  @Test
  public void testSparseMapFieldSpecDefaultNullValue() {
    SparseMapFieldSpec spec = new SparseMapFieldSpec("col");
    Object defaultNullValue = spec.getDefaultNullValue();
    assertNotNull(defaultNullValue);
    assertTrue(defaultNullValue instanceof Map);
    assertTrue(((Map<?, ?>) defaultNullValue).isEmpty());
  }

  @Test
  public void testSparseMapFieldSpecIsSingleValue() {
    SparseMapFieldSpec spec = new SparseMapFieldSpec("col");
    assertTrue(spec.isSingleValueField());
  }

  /**
   * Verifies that {@code keyTypes} and {@code defaultValueType} survive the
   * {@link SparseMapFieldSpec#toJsonObject()} → JSON → deserialization round-trip used by the
   * controller REST API.
   */
  @Test
  public void testSparseMapFieldSpecToJsonObjectIncludesKeyFields()
      throws Exception {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("clicks", FieldSpec.DataType.LONG);
    keyTypes.put("spend", FieldSpec.DataType.DOUBLE);
    keyTypes.put("country", FieldSpec.DataType.STRING);
    SparseMapFieldSpec spec = new SparseMapFieldSpec("metrics", keyTypes, FieldSpec.DataType.DOUBLE);

    // Serialise via toJsonObject() — what the controller API sees
    ObjectNode json = spec.toJsonObject();
    assertEquals(json.get("fieldType").asText(), "SPARSE_MAP");
    assertTrue(json.has("keyTypes"), "toJsonObject() must emit keyTypes");
    assertEquals(json.get("keyTypes").get("clicks").asText(), "LONG");
    assertEquals(json.get("keyTypes").get("spend").asText(), "DOUBLE");
    assertEquals(json.get("keyTypes").get("country").asText(), "STRING");
    assertTrue(json.has("defaultValueType"), "toJsonObject() must emit defaultValueType when non-STRING");
    assertEquals(json.get("defaultValueType").asText(), "DOUBLE");

    // Round-trip: deserialise the JSON back into a SparseMapFieldSpec
    SparseMapFieldSpec roundTripped = JsonUtils.jsonNodeToObject(json, SparseMapFieldSpec.class);
    assertNotNull(roundTripped.getKeyTypes());
    assertEquals(roundTripped.getKeyTypes().get("clicks"), FieldSpec.DataType.LONG);
    assertEquals(roundTripped.getKeyTypes().get("spend"), FieldSpec.DataType.DOUBLE);
    assertEquals(roundTripped.getDefaultValueType(), FieldSpec.DataType.DOUBLE);
  }

  /**
   * Verifies the full Schema round-trip: a SPARSE_MAP schema serialised via
   * {@link Schema#toJsonObject()} and parsed back via {@link Schema#fromString(String)} retains
   * the key-type declarations required by schema validation.
   */
  @Test
  public void testSchemaRoundTripPreservesSparseMapKeyTypes()
      throws Exception {
    Schema original = new Schema();
    original.setSchemaName("roundTripSchema");
    SparseMapFieldSpec spec = new SparseMapFieldSpec("metrics",
        Collections.singletonMap("clicks", FieldSpec.DataType.LONG));
    original.addField(spec);

    // Serialise → JSON string → deserialise (mirrors AddTable REST flow)
    String jsonStr = original.toJsonObject().toString();
    Schema reloaded = Schema.fromString(jsonStr);

    reloaded.validate(); // Must not throw "must declare keyTypes"
    SparseMapFieldSpec reloadedSpec = (SparseMapFieldSpec) reloaded.getFieldSpecFor("metrics");
    assertNotNull(reloadedSpec.getKeyTypes());
    assertEquals(reloadedSpec.getKeyTypes().get("clicks"), FieldSpec.DataType.LONG);
    assertEquals(reloaded.getFieldSpecFor("metrics").getFieldType(), FieldSpec.FieldType.SPARSE_MAP);
  }

  @Test
  public void testSchemaJsonContainsSparseMapFieldSpecs() {
    Schema schema = new Schema();
    schema.setSchemaName("test");
    SparseMapFieldSpec spec = new SparseMapFieldSpec("userMetrics",
        Collections.singletonMap("clicks", FieldSpec.DataType.LONG));
    schema.addField(spec);

    ObjectNode jsonObject = schema.toJsonObject();
    // Must appear in sparseMapFieldSpecs, NOT dimensionFieldSpecs
    assertTrue(jsonObject.has("sparseMapFieldSpecs"), "schema JSON must have sparseMapFieldSpecs");
    assertFalse(jsonObject.has("dimensionFieldSpecs"), "schema JSON must not have dimensionFieldSpecs for SPARSE_MAP");
    assertEquals(jsonObject.get("sparseMapFieldSpecs").get(0).get("fieldType").asText(), "SPARSE_MAP");
  }
}
