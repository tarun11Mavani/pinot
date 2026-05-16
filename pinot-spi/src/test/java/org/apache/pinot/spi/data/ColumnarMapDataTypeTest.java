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

import java.util.Map;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class ColumnarMapDataTypeTest {

  @Test
  public void testValueFieldSpecsOnComplexFieldSpec() {
    Map<String, FieldSpec> valueFieldSpecs = Map.of(
        "clicks", new MetricFieldSpec("clicks", DataType.LONG),
        "country", new DimensionFieldSpec("country", DataType.STRING, true));
    ComplexFieldSpec spec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    spec.setValueFieldSpecs(valueFieldSpecs);
    spec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    assertEquals(spec.getValueFieldSpecs().get("clicks").getDataType(), DataType.LONG);
    assertEquals(spec.getValueFieldSpecs().get("country").getDataType(), DataType.STRING);
    assertEquals(spec.getDefaultValueFieldSpec().getDataType(), DataType.STRING);
  }

  @Test
  public void testDefaultValueFieldSpecDefaultsToNull() {
    ComplexFieldSpec spec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    assertNull(spec.getValueFieldSpecs());
    assertNull(spec.getDefaultValueFieldSpec());
  }

  @Test
  public void testJsonRoundTripWithValueFieldSpecs()
      throws Exception {
    Map<String, FieldSpec> valueFieldSpecs = Map.of(
        "clicks", new MetricFieldSpec("clicks", DataType.LONG),
        "country", new DimensionFieldSpec("country", DataType.STRING, true));

    ComplexFieldSpec original = new ComplexFieldSpec("metrics", DataType.MAP, true);
    original.setValueFieldSpecs(valueFieldSpecs);
    original.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    String json = original.toJsonObject().toString();
    ComplexFieldSpec deserialized = JsonUtils.stringToObject(json, ComplexFieldSpec.class);

    assertNotNull(deserialized.getValueFieldSpecs());
    assertEquals(deserialized.getValueFieldSpecs().get("clicks").getDataType(), DataType.LONG);
    assertEquals(deserialized.getValueFieldSpecs().get("country").getDataType(), DataType.STRING);
    assertTrue(deserialized.getValueFieldSpecs().get("country").isSingleValueField());
    assertEquals(deserialized.getDefaultValueFieldSpec().getDataType(), DataType.STRING);
  }

  @Test
  public void testJsonRoundTripWithoutValueFieldSpecs()
      throws Exception {
    ComplexFieldSpec original = new ComplexFieldSpec("metrics", DataType.MAP, true);
    String json = original.toJsonObject().toString();
    ComplexFieldSpec deserialized = JsonUtils.stringToObject(json, ComplexFieldSpec.class);

    assertNull(deserialized.getValueFieldSpecs());
    assertNull(deserialized.getDefaultValueFieldSpec());
  }

  @Test
  public void testSchemaWithValueFieldSpecsRoundTrip()
      throws Exception {
    String schemaJson = "{\n"
        + "  \"schemaName\": \"testSchema\",\n"
        + "  \"complexFieldSpecs\": [\n"
        + "    {\n"
        + "      \"name\": \"metrics\",\n"
        + "      \"dataType\": \"MAP\",\n"
        + "      \"valueFieldSpecs\": {\n"
        + "        \"clicks\": {\"fieldType\": \"METRIC\", \"name\": \"clicks\", \"dataType\": \"LONG\"},\n"
        + "        \"country\": {\"name\": \"country\", \"dataType\": \"STRING\"}\n"
        + "      },\n"
        + "      \"defaultValueFieldSpec\": {\"name\": \"default\", \"dataType\": \"STRING\"}\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    Schema schema = JsonUtils.stringToObject(schemaJson, Schema.class);
    FieldSpec fieldSpec = schema.getFieldSpecFor("metrics");
    assertNotNull(fieldSpec);
    assertEquals(fieldSpec.getDataType(), DataType.MAP);

    ComplexFieldSpec complexSpec = (ComplexFieldSpec) fieldSpec;
    assertNotNull(complexSpec.getValueFieldSpecs());
    assertEquals(complexSpec.getValueFieldSpecs().get("clicks").getDataType(), DataType.LONG);
    assertEquals(complexSpec.getValueFieldSpecs().get("clicks").getFieldType(), FieldSpec.FieldType.METRIC);
    assertEquals(complexSpec.getValueFieldSpecs().get("country").getDataType(), DataType.STRING);
    assertEquals(complexSpec.getValueFieldSpecs().get("country").getFieldType(), FieldSpec.FieldType.DIMENSION);
    assertEquals(complexSpec.getDefaultValueFieldSpec().getDataType(), DataType.STRING);

    // Re-serialize and verify round-trip
    String reJson = schema.toJsonObject().toString();
    Schema reSchema = JsonUtils.stringToObject(reJson, Schema.class);
    ComplexFieldSpec reSpec = (ComplexFieldSpec) reSchema.getFieldSpecFor("metrics");
    assertEquals(reSpec.getValueFieldSpecs().get("clicks").getDataType(), DataType.LONG);
    assertEquals(reSpec.getDefaultValueFieldSpec().getDataType(), DataType.STRING);
  }
}
