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
import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;


/// Configuration for the MAP index on a MAP column.
///
/// **Dense vs sparse:** a key is materialized as its own column if (a) it appears in the explicit
/// `denseKeys` set, or (b) its fill rate (fraction of documents containing the key) is ≥
/// `denseKeyMinFillRate`. Keys not satisfying either criterion go into a sparse MAP column.
///
/// **maxDenseKeys cutoff:** when more keys qualify as dense than `maxDenseKeys` allows, the top
/// `maxDenseKeys` keys ranked by fill rate are materialized; the rest fall back to the sparse
/// column. Use `denseKeys` to pin specific keys regardless of fill rate ranking.
///
/// **Per-key index settings** are specified via `valueFieldConfigs` — each entry is a standard
/// [FieldConfig] (modern `indexes` format) for one materialized MAP key. Keys without an entry
/// use defaults: DICTIONARY encoding and no per-key inverted index unless
/// `enableInvertedIndexForDense` is `true`.
public class MapIndexConfig extends IndexConfig {
  public static final MapIndexConfig DISABLED = new MapIndexConfig(false);
  public static final MapIndexConfig DEFAULT = new MapIndexConfig(true);

  public static final double DEFAULT_DENSE_KEY_MIN_FILL_RATE = 0.5;
  public static final int DEFAULT_MAX_DENSE_KEYS = 1000;
  private static final String INVERTED_INDEX_KEY = "inverted";

  private final boolean _enableInvertedIndexForDense;
  private final int _maxDenseKeys;
  private final Set<String> _denseKeys;
  private final double _denseKeyMinFillRate;
  private final List<FieldConfig> _valueFieldConfigs;
  // Eager lookup from key name → FieldConfig for O(1) per-key access. Built in constructor
  // so the config is fully immutable and safe to share across threads.
  private final Map<String, FieldConfig> _valueFieldConfigIndex;

  public MapIndexConfig(boolean enabled) {
    this(!enabled, false, DEFAULT_MAX_DENSE_KEYS, null, DEFAULT_DENSE_KEY_MIN_FILL_RATE, null);
  }

  @JsonCreator
  public MapIndexConfig(
      @JsonProperty("disabled") Boolean disabled,
      @JsonProperty("enableInvertedIndexForDense") boolean enableInvertedIndexForDense,
      @JsonProperty("maxDenseKeys") int maxDenseKeys,
      @JsonProperty("denseKeys") @Nullable Set<String> denseKeys,
      @JsonProperty("denseKeyMinFillRate") @Nullable Double denseKeyMinFillRate,
      @JsonProperty("valueFieldConfigs") @Nullable List<FieldConfig> valueFieldConfigs) {
    super(disabled);
    _enableInvertedIndexForDense = enableInvertedIndexForDense;
    _maxDenseKeys = maxDenseKeys > 0 ? maxDenseKeys : DEFAULT_MAX_DENSE_KEYS;
    _denseKeys = denseKeys;
    _denseKeyMinFillRate = denseKeyMinFillRate != null ? denseKeyMinFillRate : DEFAULT_DENSE_KEY_MIN_FILL_RATE;
    _valueFieldConfigs = valueFieldConfigs;
    if (valueFieldConfigs == null || valueFieldConfigs.isEmpty()) {
      _valueFieldConfigIndex = Map.of();
    } else {
      Map<String, FieldConfig> index = new HashMap<>(valueFieldConfigs.size());
      for (FieldConfig fc : valueFieldConfigs) {
        index.put(fc.getName(), fc);
      }
      _valueFieldConfigIndex = index;
    }
  }

  public boolean isEnableInvertedIndexForDense() {
    return _enableInvertedIndexForDense;
  }

  /// Maximum number of MAP keys to materialise as dense columns. Default: 1000.
  /// When more keys qualify as dense, the top `maxDenseKeys` by fill rate are materialized;
  /// the rest fall back to the sparse MAP column.
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

  /// Per-key index settings. Each entry is a standard [FieldConfig] whose `name` matches a MAP
  /// key name. Keys without an entry use defaults (DICTIONARY encoding, no per-key inverted
  /// index unless `enableInvertedIndexForDense` is `true`).
  @Nullable
  public List<FieldConfig> getValueFieldConfigs() {
    return _valueFieldConfigs;
  }

  /// Returns the [FieldConfig] for the given key, or null if none was configured.
  @Nullable
  public FieldConfig getValueFieldConfig(String key) {
    return _valueFieldConfigIndex.get(key);
  }

  /// `true` if the given key should be built with an inverted index. The global
  /// `enableInvertedIndexForDense` flag wins if set; otherwise the per-key [FieldConfig]'s
  /// `indexes.inverted` decides.
  public boolean shouldEnableInvertedIndexForKey(String key) {
    if (_enableInvertedIndexForDense) {
      return true;
    }
    FieldConfig keyConfig = getValueFieldConfig(key);
    if (keyConfig == null) {
      return false;
    }
    JsonNode indexes = keyConfig.getIndexes();
    return indexes != null && indexes.isObject() && indexes.has(INVERTED_INDEX_KEY);
  }

  /// `true` if the given key should be dictionary-encoded. Defaults to `true` (dictionary) when
  /// no per-key [FieldConfig] is set or when its `encodingType` is null/DICTIONARY.
  public boolean shouldUseDictionaryForKey(String key) {
    FieldConfig keyConfig = getValueFieldConfig(key);
    if (keyConfig == null) {
      return true;
    }
    return keyConfig.getEncodingType() != FieldConfig.EncodingType.RAW;
  }
}
