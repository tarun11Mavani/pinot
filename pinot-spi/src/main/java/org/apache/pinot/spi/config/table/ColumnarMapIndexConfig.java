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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;


/**
 * Configuration for the COLUMNAR_MAP index on a MAP column.
 *
 * <p>Dense keys (above {@code denseKeyMinFillRate} or explicitly listed in {@code denseKeys}) are
 * stored as independent virtual columns with a standard forward index, dictionary, and optional
 * inverted index. At most {@code maxDenseKeys} keys are materialised as dense virtual columns;
 * remaining keys fall back to a synthetic JSON sparse column.
 */
public class ColumnarMapIndexConfig extends IndexConfig {
  public static final ColumnarMapIndexConfig DISABLED = new ColumnarMapIndexConfig(false);
  public static final ColumnarMapIndexConfig DEFAULT = new ColumnarMapIndexConfig(true);

  public static final double DEFAULT_DENSE_KEY_MIN_FILL_RATE = 0.5;

  private final boolean _enableInvertedIndexForDense;
  private final Set<String> _invertedIndexKeys;
  private final Set<String> _noDictionaryKeys;
  private final int _maxDenseKeys;
  private final Set<String> _denseKeys;
  private final double _denseKeyMinFillRate;

  public static ColumnarMapIndexConfig fromProperties(@Nullable Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return DEFAULT;
    }
    int maxDenseKeys = Integer.parseInt(
        properties.getOrDefault(FieldConfig.COLUMNAR_MAP_INDEX_MAX_DENSE_KEYS, "1000"));
    Set<String> invertedIndexKeys = parseCommaSeparated(
        properties.get(FieldConfig.COLUMNAR_MAP_INDEX_INVERTED_INDEX_KEYS));
    Set<String> noDictionaryKeys = parseCommaSeparated(
        properties.get(FieldConfig.COLUMNAR_MAP_INDEX_NO_DICTIONARY_KEYS));
    boolean enableInvertedForDense = Boolean.parseBoolean(
        properties.getOrDefault(FieldConfig.COLUMNAR_MAP_INDEX_ENABLE_INVERTED_FOR_DENSE, "false"));
    Set<String> denseKeys = parseCommaSeparated(
        properties.get(FieldConfig.COLUMNAR_MAP_INDEX_DENSE_KEYS));
    double denseKeyMinFillRate = Double.parseDouble(
        properties.getOrDefault(FieldConfig.COLUMNAR_MAP_INDEX_DENSE_KEY_MIN_FILL_RATE,
            String.valueOf(DEFAULT_DENSE_KEY_MIN_FILL_RATE)));
    return new ColumnarMapIndexConfig(true, enableInvertedForDense, invertedIndexKeys, noDictionaryKeys, maxDenseKeys,
        denseKeys, denseKeyMinFillRate);
  }

  @Nullable
  private static Set<String> parseCommaSeparated(@Nullable String value) {
    if (value == null || value.trim().isEmpty()) {
      return null;
    }
    Set<String> result = new HashSet<>();
    for (String part : value.split(FieldConfig.COLUMNAR_MAP_INDEX_KEY_SEPARATOR)) {
      String trimmed = part.trim();
      if (!trimmed.isEmpty()) {
        result.add(trimmed);
      }
    }
    return result.isEmpty() ? null : result;
  }

  public ColumnarMapIndexConfig(boolean enabled) {
    this(enabled, false, null, null, 1000, null, DEFAULT_DENSE_KEY_MIN_FILL_RATE);
  }

  @JsonCreator
  public ColumnarMapIndexConfig(
      @JsonProperty("enabled") boolean enabled,
      @JsonProperty("enableInvertedIndexForDense") boolean enableInvertedIndexForDense,
      @JsonProperty("invertedIndexKeys") @Nullable Set<String> invertedIndexKeys,
      @JsonProperty("noDictionaryKeys") @Nullable Set<String> noDictionaryKeys,
      @JsonProperty("maxDenseKeys") int maxDenseKeys,
      @JsonProperty("denseKeys") @Nullable Set<String> denseKeys,
      @JsonProperty("denseKeyMinFillRate") double denseKeyMinFillRate) {
    super(!enabled);
    _enableInvertedIndexForDense = enableInvertedIndexForDense;
    _invertedIndexKeys = invertedIndexKeys;
    _noDictionaryKeys = noDictionaryKeys;
    _maxDenseKeys = maxDenseKeys > 0 ? maxDenseKeys : 1000;
    _denseKeys = denseKeys;
    _denseKeyMinFillRate = denseKeyMinFillRate >= 0 ? denseKeyMinFillRate : DEFAULT_DENSE_KEY_MIN_FILL_RATE;
  }

  public boolean isEnableInvertedIndexForDense() {
    return _enableInvertedIndexForDense;
  }

  @Nullable
  public Set<String> getInvertedIndexKeys() {
    return _invertedIndexKeys;
  }

  public boolean shouldEnableInvertedIndexForKey(String key) {
    return _enableInvertedIndexForDense
        || (_invertedIndexKeys != null && _invertedIndexKeys.contains(key));
  }

  @Nullable
  public Set<String> getNoDictionaryKeys() {
    return _noDictionaryKeys;
  }

  public boolean shouldUseDictionaryForKey(String key) {
    return _noDictionaryKeys == null || !_noDictionaryKeys.contains(key);
  }

  /** Maximum number of MAP keys to materialise as dense virtual columns. Default: 1000. */
  public int getMaxDenseKeys() {
    return _maxDenseKeys;
  }

  public Set<String> getDenseKeys() {
    return _denseKeys != null ? _denseKeys : Set.of();
  }

  public double getDenseKeyMinFillRate() {
    return _denseKeyMinFillRate;
  }

  public boolean isDenseKey(String key) {
    return _denseKeys != null && _denseKeys.contains(key);
  }
}
