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
package org.apache.pinot.segment.spi.index.reader;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/// Reader for the MAP index. Each indexed key is materialized as its own per-key
/// forward index plus a presence bitmap.
///
/// Implementations must be safe for concurrent reads. Mutable implementations may impose
/// a single-writer constraint; refer to the concrete implementation's Javadoc for details.
///
/// Per-key `DataSource` construction is the responsibility of the surrounding
/// `ColumnarMapDataSource` wrappers, not this reader. This interface exposes only
/// the primitives a wrapper needs (key set, type, presence bitmap, per-doc map view).
public interface ColumnarMapIndexReader extends IndexReader {

  /// Returns the set of all indexed key names. Never null; empty if no keys are indexed.
  Set<String> getKeys();

  /// Returns the value DataType for the given key, or null if the key is not indexed.
  @Nullable
  DataType getValueType(String key);

  /// Returns the number of documents that have a non-null value for the given key.
  /// Returns 0 if the key is not indexed.
  int getNumDocsWithKey(String key);

  /// Returns the presence bitmap for the given key (docIds with non-null values).
  /// Returns an empty bitmap if the key is not indexed. The returned bitmap must not be mutated.
  ImmutableRoaringBitmap getPresenceBitmap(String key);

  /// Reconstructs the full map for a single document from per-key data. Only keys with a
  /// non-null value at `docId` appear in the result. Returns an empty map if the document has
  /// no values; behavior for an out-of-range `docId` is implementation-defined.
  Map<String, Object> getMap(int docId);

  /// Returns whether the given key has an inverted index available. False if the key is not indexed.
  default boolean hasInvertedIndex(String key) {
    return false;
  }

  /// Returns sorted distinct values for the key from the inverted index, or null if no
  /// inverted index is available (or the key is not indexed).
  @Nullable
  default String[] getDistinctValuesForKey(String key) {
    return null;
  }
}
