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
package org.apache.pinot.spi.config.table;

import java.util.List;
import java.util.Set;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class MapIndexConfigTest {

  @Test
  public void testDefaultConfig() {
    MapIndexConfig config = MapIndexConfig.DEFAULT;
    assertTrue(config.isEnabled());
    assertEquals(config.getMaxDenseKeys(), 0);
    assertEquals(config.getDenseKeyMinFillRate(), 0.5);
    assertTrue(config.getDenseKeys().isEmpty());
    assertNull(config.getValueFieldConfigs());
    assertFalse(config.isEnableInvertedIndexForDense());
  }

  @Test
  public void testDisabledConfig() {
    MapIndexConfig config = MapIndexConfig.DISABLED;
    assertFalse(config.isEnabled());
  }

  @Test
  public void testNoDictionaryKeys() {
    FieldConfig rawKey =
        new FieldConfig("raw_payload", FieldConfig.EncodingType.RAW, (List<FieldConfig.IndexType>) null, null, null);
    MapIndexConfig config = new MapIndexConfig(false, false, 1000, null, 0.5, List.of(rawKey));
    assertFalse(config.shouldUseDictionaryForKey("raw_payload"));
    assertTrue(config.shouldUseDictionaryForKey("other_key"));
  }

  @Test
  public void testShouldEnableInvertedIndexForKeyGlobalFlag() {
    MapIndexConfig config = new MapIndexConfig(false, true, 1000, null, 0.5, null);
    assertTrue(config.shouldEnableInvertedIndexForKey("any_key"));
  }

  @Test
  public void testShouldEnableInvertedIndexForKeyPerKeyOnly()
      throws Exception {
    FieldConfig country = JsonUtils.stringToObject(
        "{\"name\":\"country\",\"indexes\":{\"inverted\":{}}}", FieldConfig.class);
    FieldConfig clicks = JsonUtils.stringToObject(
        "{\"name\":\"clicks\",\"indexes\":{\"inverted\":{}}}", FieldConfig.class);
    MapIndexConfig config = new MapIndexConfig(false, false, 1000, null, 0.5,
        List.of(country, clicks));
    assertTrue(config.shouldEnableInvertedIndexForKey("country"));
    assertTrue(config.shouldEnableInvertedIndexForKey("clicks"));
    assertFalse(config.shouldEnableInvertedIndexForKey("other"));
  }

  @Test
  public void testShouldEnableInvertedIndexForKeyUnion()
      throws Exception {
    FieldConfig country = JsonUtils.stringToObject(
        "{\"name\":\"country\",\"indexes\":{\"inverted\":{}}}", FieldConfig.class);
    MapIndexConfig config = new MapIndexConfig(false, true, 1000, null, 0.5, List.of(country));
    assertTrue(config.shouldEnableInvertedIndexForKey("country"));
    assertTrue(config.shouldEnableInvertedIndexForKey("other"));
  }

  @Test
  public void testShouldEnableInvertedIndexForKeyHonorsDisabledFlag()
      throws Exception {
    FieldConfig country = JsonUtils.stringToObject(
        "{\"name\":\"country\",\"indexes\":{\"inverted\":{\"disabled\":true}}}", FieldConfig.class);
    FieldConfig clicks = JsonUtils.stringToObject(
        "{\"name\":\"clicks\",\"indexes\":{\"inverted\":{\"disabled\":false}}}", FieldConfig.class);
    MapIndexConfig config = new MapIndexConfig(false, false, 1000, null, 0.5, List.of(country, clicks));
    assertFalse(config.shouldEnableInvertedIndexForKey("country"));
    assertTrue(config.shouldEnableInvertedIndexForKey("clicks"));
  }

  @Test
  public void testShouldUseDictionaryForKeyHardOverride() {
    FieldConfig blob =
        new FieldConfig("blob", FieldConfig.EncodingType.RAW, (List<FieldConfig.IndexType>) null, null, null);
    FieldConfig rawPayload =
        new FieldConfig("raw_payload", FieldConfig.EncodingType.RAW, (List<FieldConfig.IndexType>) null, null, null);
    MapIndexConfig config = new MapIndexConfig(false, false, 1000, null, 0.5, List.of(blob, rawPayload));
    assertFalse(config.shouldUseDictionaryForKey("blob"));
    assertFalse(config.shouldUseDictionaryForKey("raw_payload"));
    assertTrue(config.shouldUseDictionaryForKey("country"));
  }

  @Test
  public void testValueFieldConfigsRoundTrip()
      throws Exception {
    String json = "{\n"
        + "  \"maxDenseKeys\": 500,\n"
        + "  \"denseKeyMinFillRate\": 0.3,\n"
        + "  \"denseKeys\": [\"country\", \"clicks\"],\n"
        + "  \"enableInvertedIndexForDense\": false,\n"
        + "  \"valueFieldConfigs\": [\n"
        + "    {\n"
        + "      \"name\": \"country\",\n"
        + "      \"encodingType\": \"DICTIONARY\",\n"
        + "      \"indexes\": {\"inverted\": {}}\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"clicks\",\n"
        + "      \"encodingType\": \"RAW\"\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    MapIndexConfig config = JsonUtils.stringToObject(json, MapIndexConfig.class);

    assertEquals(config.getMaxDenseKeys(), 500);
    assertEquals(config.getDenseKeyMinFillRate(), 0.3);
    assertEquals(config.getDenseKeys(), Set.of("country", "clicks"));

    List<FieldConfig> valueFieldConfigs = config.getValueFieldConfigs();
    assertNotNull(valueFieldConfigs);
    assertEquals(valueFieldConfigs.size(), 2);

    // country: dictionary + inverted index
    assertTrue(config.shouldUseDictionaryForKey("country"));
    assertTrue(config.shouldEnableInvertedIndexForKey("country"));

    // clicks: raw, no inverted
    assertFalse(config.shouldUseDictionaryForKey("clicks"));
    assertFalse(config.shouldEnableInvertedIndexForKey("clicks"));

    // unconfigured key: defaults (dictionary, no inverted)
    assertTrue(config.shouldUseDictionaryForKey("payload"));
    assertFalse(config.shouldEnableInvertedIndexForKey("payload"));

    // direct lookup
    assertNotNull(config.getValueFieldConfig("country"));
    assertEquals(config.getValueFieldConfig("country").getEncodingType(), FieldConfig.EncodingType.DICTIONARY);
    assertNull(config.getValueFieldConfig("missing"));

    // JSON serialization round-trip
    String reJson = JsonUtils.objectToString(config);
    MapIndexConfig reDeserialized = JsonUtils.stringToObject(reJson, MapIndexConfig.class);
    assertEquals(reDeserialized.getMaxDenseKeys(), 500);
    assertEquals(reDeserialized.getDenseKeys(), Set.of("country", "clicks"));
    assertNotNull(reDeserialized.getValueFieldConfigs());
    assertEquals(reDeserialized.getValueFieldConfigs().size(), 2);
    assertTrue(reDeserialized.shouldUseDictionaryForKey("country"));
    assertTrue(reDeserialized.shouldEnableInvertedIndexForKey("country"));
    assertFalse(reDeserialized.shouldUseDictionaryForKey("clicks"));
  }

  @Test
  public void testEmptyValueFieldConfigs()
      throws Exception {
    String json = "{\"valueFieldConfigs\": []}";
    MapIndexConfig config = JsonUtils.stringToObject(json, MapIndexConfig.class);
    assertNotNull(config.getValueFieldConfigs());
    assertTrue(config.getValueFieldConfigs().isEmpty());
    assertNull(config.getValueFieldConfig("any"));
    assertTrue(config.shouldUseDictionaryForKey("any"));
    assertFalse(config.shouldEnableInvertedIndexForKey("any"));
  }

  @Test
  public void testDisabledViaJson()
      throws Exception {
    String json = "{\"disabled\": true}";
    MapIndexConfig config = JsonUtils.stringToObject(json, MapIndexConfig.class);
    assertFalse(config.isEnabled());
  }

  @Test
  public void testEmptyJsonDefaults()
      throws Exception {
    MapIndexConfig config = JsonUtils.stringToObject("{}", MapIndexConfig.class);
    assertTrue(config.isEnabled());
    assertEquals(config.getMaxDenseKeys(), MapIndexConfig.DEFAULT_MAX_DENSE_KEYS);
    assertEquals(config.getDenseKeyMinFillRate(), MapIndexConfig.DEFAULT_DENSE_KEY_MIN_FILL_RATE);
    assertTrue(config.getDenseKeys().isEmpty());
    assertNull(config.getValueFieldConfigs());
    assertFalse(config.isEnableInvertedIndexForDense());
  }

  @Test
  public void testPartialJsonRetainsDefaultFillRate()
      throws Exception {
    MapIndexConfig config = JsonUtils.stringToObject("{\"maxDenseKeys\": 500}", MapIndexConfig.class);
    assertEquals(config.getMaxDenseKeys(), 500);
    assertEquals(config.getDenseKeyMinFillRate(), MapIndexConfig.DEFAULT_DENSE_KEY_MIN_FILL_RATE);
  }
}
