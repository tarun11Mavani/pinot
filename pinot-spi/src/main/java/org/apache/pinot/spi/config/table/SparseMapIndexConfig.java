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
import java.util.Set;
import javax.annotation.Nullable;


/**
 * Configuration for the SparseMap index on a SPARSE_MAP column.
 * Controls which keys are indexed per-key in columnar storage and whether
 * per-key inverted indexes are enabled for fast value-based filtering.
 */
public class SparseMapIndexConfig extends IndexConfig {
  public static final SparseMapIndexConfig DISABLED = new SparseMapIndexConfig(false);
  public static final SparseMapIndexConfig DEFAULT = new SparseMapIndexConfig(true);

  private final Set<String> _indexedKeys;
  private final boolean _enableInvertedIndex;
  private final int _maxKeys;

  public SparseMapIndexConfig(boolean enabled) {
    this(enabled, null, false, 1000);
  }

  @JsonCreator
  public SparseMapIndexConfig(
      @JsonProperty("enabled") boolean enabled,
      @JsonProperty("indexedKeys") @Nullable Set<String> indexedKeys,
      @JsonProperty("enableInvertedIndex") boolean enableInvertedIndex,
      @JsonProperty("maxKeys") int maxKeys) {
    super(!enabled);
    _indexedKeys = indexedKeys;
    _enableInvertedIndex = enableInvertedIndex;
    _maxKeys = maxKeys > 0 ? maxKeys : 1000;
  }

  /**
   * Returns the set of keys to index, or null if all keys should be indexed.
   */
  @Nullable
  public Set<String> getIndexedKeys() {
    return _indexedKeys;
  }

  /**
   * Returns true if per-key inverted indexes should be created for fast value-based filtering.
   */
  public boolean isEnableInvertedIndex() {
    return _enableInvertedIndex;
  }

  /**
   * Returns the maximum number of distinct keys allowed. Keys beyond this cap fall back to
   * the forward index blob. Default is 1000.
   */
  public int getMaxKeys() {
    return _maxKeys;
  }
}
