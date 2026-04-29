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


public class ColumnarMapDataTypeTest {

  @Test
  public void testKeyTypesOnComplexFieldSpec() {
    Map<String, DataType> keyTypes = Map.of("clicks", DataType.LONG, "country", DataType.STRING);
    ComplexFieldSpec spec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    spec.setKeyTypes(keyTypes);
    spec.setDefaultValueType(DataType.STRING);

    assertEquals(spec.getKeyTypes(), keyTypes);
    assertEquals(spec.getDefaultValueType(), DataType.STRING);
  }

  @Test
  public void testDefaultValueTypeDefaultsToNull() {
    ComplexFieldSpec spec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    assertNull(spec.getKeyTypes());
    assertNull(spec.getDefaultValueType());
  }

  @Test
  public void testJsonRoundTripWithKeyTypes()
      throws Exception {
    Map<String, DataType> keyTypes = Map.of("clicks", DataType.LONG, "country", DataType.STRING);

    ComplexFieldSpec original = new ComplexFieldSpec("metrics", DataType.MAP, true);
    original.setKeyTypes(keyTypes);
    original.setDefaultValueType(DataType.STRING);

    String json = original.toJsonObject().toString();
    ComplexFieldSpec deserialized = JsonUtils.stringToObject(json, ComplexFieldSpec.class);

    assertNotNull(deserialized.getKeyTypes());
    assertEquals(deserialized.getKeyTypes().get("clicks"), DataType.LONG);
    assertEquals(deserialized.getKeyTypes().get("country"), DataType.STRING);
    assertEquals(deserialized.getDefaultValueType(), DataType.STRING);
  }

  @Test
  public void testJsonRoundTripWithoutKeyTypes()
      throws Exception {
    ComplexFieldSpec original = new ComplexFieldSpec("metrics", DataType.MAP, true);
    String json = original.toJsonObject().toString();
    ComplexFieldSpec deserialized = JsonUtils.stringToObject(json, ComplexFieldSpec.class);

    assertNull(deserialized.getKeyTypes());
    assertNull(deserialized.getDefaultValueType());
  }

  @Test
  public void testSchemaWithKeyTypesRoundTrip()
      throws Exception {
    String schemaJson = "{\n"
        + "  \"schemaName\": \"testSchema\",\n"
        + "  \"complexFieldSpecs\": [\n"
        + "    {\n"
        + "      \"name\": \"metrics\",\n"
        + "      \"dataType\": \"MAP\",\n"
        + "      \"keyTypes\": {\"clicks\": \"LONG\", \"country\": \"STRING\"},\n"
        + "      \"defaultValueType\": \"STRING\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    Schema schema = JsonUtils.stringToObject(schemaJson, Schema.class);
    FieldSpec fieldSpec = schema.getFieldSpecFor("metrics");
    assertNotNull(fieldSpec);
    assertEquals(fieldSpec.getDataType(), DataType.MAP);

    ComplexFieldSpec complexSpec = (ComplexFieldSpec) fieldSpec;
    assertNotNull(complexSpec.getKeyTypes());
    assertEquals(complexSpec.getKeyTypes().get("clicks"), DataType.LONG);
    assertEquals(complexSpec.getKeyTypes().get("country"), DataType.STRING);
    assertEquals(complexSpec.getDefaultValueType(), DataType.STRING);

    // Re-serialize and verify round-trip
    String reJson = schema.toJsonObject().toString();
    Schema reSchema = JsonUtils.stringToObject(reJson, Schema.class);
    ComplexFieldSpec reSpec = (ComplexFieldSpec) reSchema.getFieldSpecFor("metrics");
    assertEquals(reSpec.getKeyTypes().get("clicks"), DataType.LONG);
    assertEquals(reSpec.getDefaultValueType(), DataType.STRING);
  }
}
