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

import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class ColumnarMapIndexConfigTest {

  @Test
  public void testDefaultConfig() {
    ColumnarMapIndexConfig config = ColumnarMapIndexConfig.DEFAULT;
    assertTrue(config.isEnabled());
    assertEquals(config.getMaxKeys(), 1000);
    assertEquals(config.getDenseKeyMinFillRate(), 0.5);
    assertTrue(config.getDenseKeys().isEmpty());
    assertNull(config.getInvertedIndexKeys());
    assertNull(config.getNoDictionaryKeys());
    assertFalse(config.isEnableInvertedIndexForDense());
  }

  @Test
  public void testFromProperties() {
    Map<String, String> props = Map.of(
        FieldConfig.COLUMNAR_MAP_INDEX_MAX_KEYS, "500",
        FieldConfig.COLUMNAR_MAP_INDEX_DENSE_KEYS, "country,tenancy",
        FieldConfig.COLUMNAR_MAP_INDEX_DENSE_KEY_MIN_FILL_RATE, "0.3",
        FieldConfig.COLUMNAR_MAP_INDEX_INVERTED_INDEX_KEYS, "country",
        FieldConfig.COLUMNAR_MAP_INDEX_ENABLE_INVERTED_FOR_DENSE, "false"
    );
    ColumnarMapIndexConfig config = ColumnarMapIndexConfig.fromProperties(props);
    assertTrue(config.isEnabled());
    assertEquals(config.getMaxKeys(), 500);
    assertEquals(config.getDenseKeyMinFillRate(), 0.3);
    assertEquals(config.getDenseKeys(), Set.of("country", "tenancy"));
    assertTrue(config.isDenseKey("country"));
    assertTrue(config.shouldEnableInvertedIndexForKey("country"));
    assertFalse(config.shouldEnableInvertedIndexForKey("tenancy"));
  }

  @Test
  public void testFromPropertiesEnableInvertedForDense() {
    Map<String, String> props = Map.of(
        FieldConfig.COLUMNAR_MAP_INDEX_ENABLE_INVERTED_FOR_DENSE, "true"
    );
    ColumnarMapIndexConfig config = ColumnarMapIndexConfig.fromProperties(props);
    assertTrue(config.isEnableInvertedIndexForDense());
    assertTrue(config.shouldEnableInvertedIndexForKey("anyKey"));
  }

  @Test
  public void testDisabledConfig() {
    ColumnarMapIndexConfig config = ColumnarMapIndexConfig.DISABLED;
    assertFalse(config.isEnabled());
  }

  @Test
  public void testNoDictionaryKeys() {
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, false, null,
        Set.of("raw_payload"), 1000, null, 0.5);
    assertFalse(config.shouldUseDictionaryForKey("raw_payload"));
    assertTrue(config.shouldUseDictionaryForKey("other_key"));
  }

  @Test
  public void testFromPropertiesDefaults() {
    ColumnarMapIndexConfig config = ColumnarMapIndexConfig.fromProperties(null);
    assertTrue(config.isEnabled());
    assertEquals(config.getMaxKeys(), 1000);
    assertEquals(config.getDenseKeyMinFillRate(), 0.5);
  }

  @Test
  public void testShouldEnableInvertedIndexForKeyGlobalFlag() {
    ColumnarMapIndexConfig config = ColumnarMapIndexConfig.fromProperties(
        Map.of(
            FieldConfig.COLUMNAR_MAP_INDEX_ENABLE_INVERTED_FOR_DENSE, "true"
        ));
    assertTrue(config.shouldEnableInvertedIndexForKey("any_key"));
  }

  @Test
  public void testShouldEnableInvertedIndexForKeyPerKeyOnly() {
    ColumnarMapIndexConfig config = ColumnarMapIndexConfig.fromProperties(
        Map.of(
            FieldConfig.COLUMNAR_MAP_INDEX_ENABLE_INVERTED_FOR_DENSE, "false",
            FieldConfig.COLUMNAR_MAP_INDEX_INVERTED_INDEX_KEYS, "country,clicks"
        ));
    assertTrue(config.shouldEnableInvertedIndexForKey("country"));
    assertTrue(config.shouldEnableInvertedIndexForKey("clicks"));
    assertFalse(config.shouldEnableInvertedIndexForKey("other"));
  }

  @Test
  public void testShouldEnableInvertedIndexForKeyUnion() {
    ColumnarMapIndexConfig config = ColumnarMapIndexConfig.fromProperties(
        Map.of(
            FieldConfig.COLUMNAR_MAP_INDEX_ENABLE_INVERTED_FOR_DENSE, "true",
            FieldConfig.COLUMNAR_MAP_INDEX_INVERTED_INDEX_KEYS, "country"
        ));
    assertTrue(config.shouldEnableInvertedIndexForKey("country"));
    assertTrue(config.shouldEnableInvertedIndexForKey("other"));
  }

  @Test
  public void testShouldUseDictionaryForKeyHardOverride() {
    ColumnarMapIndexConfig config = ColumnarMapIndexConfig.fromProperties(
        Map.of(
            FieldConfig.COLUMNAR_MAP_INDEX_NO_DICTIONARY_KEYS, "blob,raw_payload"
        ));
    assertFalse(config.shouldUseDictionaryForKey("blob"));
    assertFalse(config.shouldUseDictionaryForKey("raw_payload"));
    assertTrue(config.shouldUseDictionaryForKey("country"));
  }
}
