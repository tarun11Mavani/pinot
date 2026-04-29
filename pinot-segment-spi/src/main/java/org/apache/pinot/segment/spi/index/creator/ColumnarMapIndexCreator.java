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
package org.apache.pinot.segment.spi.index.creator;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.segment.spi.index.IndexCreator;


/**
 * Creator for the COLUMNAR_MAP index. Accepts one map per document during segment creation
 * and decomposes it into per-key columnar storage on seal().
 *
 * <p>Implementations are not thread-safe; callers must serialize {@link #add} calls per
 * creator instance.
 *
 * <p>The inherited {@code add(Object, int)} method from {@link IndexCreator} treats the
 * first argument as the map and the second as the docId, matching the column-major creator
 * path. Callers may use either entry point.
 */
public interface ColumnarMapIndexCreator extends IndexCreator {

  /**
   * Adds one document's map. Keys present in the map's entry set are routed to per-key
   * columnar storage; keys with declared types are coerced to those types, others fall
   * back to the configured default value type. A null or empty map is valid and means the
   * document has no key/value pairs.
   *
   * @param mapValue the document's map (may be null or empty)
   * @param docId the document id, must be monotonically non-decreasing across calls
   */
  void add(@Nullable Map<String, Object> mapValue, int docId)
      throws IOException;

  /**
   * Returns metadata properties for any virtual columns this creator materialized during
   * {@code seal()}. The framework merges the returned properties into the segment metadata.
   * Implementations that do not produce virtual columns return an empty map.
   *
   * <p>Call after {@code seal()}.
   *
   * @return a map from virtual-column name to its {@link PropertiesConfiguration}; never null
   */
  default Map<String, PropertiesConfiguration> getVirtualColumnMetadata() {
    return Collections.emptyMap();
  }
}
