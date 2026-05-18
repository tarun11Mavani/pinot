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
package org.apache.pinot.segment.spi.datasource;

import java.util.Map;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.spi.data.ComplexFieldSpec;


/// DataSource for a MAP column. Provides per-key DataSources that can be used for filtering,
/// aggregation, and projection on individual MAP keys.
public interface MapDataSource extends DataSource {

  /// Returns the map FieldSpec.
  ComplexFieldSpec.MapFieldSpec getFieldSpec();

  /// Returns the DataSource for the given map key's values.
  DataSource getDataSource(String key);

  /// Returns whether this segment has per-key index data for the given key. Columnar segments
  /// return an exact answer (O(1) lookup into the materialized key set). Blob-only segments
  /// return `true` conservatively because determining key presence requires deserialization.
  ///
  /// Query operators use this to choose between fast-path (per-key inverted/dictionary index)
  /// and fallback (expression scan). When this returns `false`, callers can short-circuit:
  /// e.g. `MapFilterOperator` returns `EmptyFilterOperator` for value predicates and
  /// `MatchAllFilterOperator` for IS_NULL. When this returns `true`, callers should
  /// still handle absent-key DataSources gracefully (null-value bitmap marks all rows as null).
  default boolean containsKey(String key) {
    return true;
  }

  /// Returns DataSources for all keys present in this segment.
  Map<String, DataSource> getDataSources();

  /// Returns the DataSourceMetadata for the given key's values.
  DataSourceMetadata getDataSourceMetadata(String key);

  /// Returns the ColumnIndexContainer for the given key's values.
  ColumnIndexContainer getIndexContainer(String key);
}
