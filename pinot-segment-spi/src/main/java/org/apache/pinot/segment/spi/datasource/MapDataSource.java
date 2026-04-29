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

  /// Returns whether this segment MAY contain the given key. Implementations are allowed to return
  /// `true` conservatively (i.e., when it is not possible to determine key presence without a
  /// full scan). Callers must handle the case where the key is absent even when this returns
  /// `true` — [#getDataSource(String)] will return a DataSource for an absent key
  /// (forward-index reads return the column default value; null-value bitmap marks all rows as null).
  default boolean containsKey(String key) {
    return getDataSources().containsKey(key);
  }

  /// Returns DataSources for all keys present in this segment.
  Map<String, DataSource> getDataSources();

  /// Returns the DataSourceMetadata for the given key's values.
  DataSourceMetadata getDataSourceMetadata(String key);

  /// Returns the ColumnIndexContainer for the given key's values.
  ColumnIndexContainer getIndexContainer(String key);
}
