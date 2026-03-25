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
package org.apache.pinot.segment.local.segment.index.sparsemap;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.segment.local.segment.index.datasource.BaseDataSource;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.MapDataSource;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.SparseMapIndexReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Data source for SPARSE_MAP columns. Wraps a {@link SparseMapIndexReader} and exposes it for
 * query access.
 *
 * <p>Implements {@link MapDataSource} so that {@code metrics['key']} expressions handled by
 * {@code ItemTransformFunction} can resolve per-key {@link DataSource} instances via
 * {@link #getKeyDataSource(String)}.  Each per-key data source wraps a
 * {@link SparseMapKeyForwardIndexReader} that delegates typed reads directly to the underlying
 * {@link SparseMapIndexReader}.
 */
public class SparseMapDataSource extends BaseDataSource implements MapDataSource {

  private final SparseMapIndexReader _sparseMapIndexReader;
  private final Map<String, DataSource> _keyDataSourceCache = new ConcurrentHashMap<>();

  /**
   * Constructs a SparseMapDataSource for an immutable segment.
   */
  public SparseMapDataSource(ColumnMetadata columnMetadata, SparseMapIndexReader sparseMapIndexReader) {
    super(new SparseMapDataSourceMetadata(columnMetadata),
        buildContainerWithForwardIndex(sparseMapIndexReader));
    _sparseMapIndexReader = sparseMapIndexReader;
  }

  /**
   * Constructs a SparseMapDataSource for a mutable (real-time) segment.
   */
  public SparseMapDataSource(FieldSpec fieldSpec, int numDocs, SparseMapIndexReader sparseMapIndexReader) {
    super(new MutableSparseMapDataSourceMetadata(fieldSpec, numDocs),
        buildContainerWithForwardIndex(sparseMapIndexReader));
    _sparseMapIndexReader = sparseMapIndexReader;
  }

  private static ColumnIndexContainer buildContainerWithForwardIndex(SparseMapIndexReader reader) {
    return new ColumnIndexContainer.FromMap.Builder()
        .with(StandardIndexes.forward(), new SparseMapForwardIndexReader(reader))
        .build();
  }

  /**
   * Returns the SparseMapIndexReader for this column.
   */
  public SparseMapIndexReader getSparseMapIndexReader() {
    return _sparseMapIndexReader;
  }

  // ---- MapDataSource implementation ----

  /**
   * SPARSE_MAP columns use {@link org.apache.pinot.spi.data.SparseMapFieldSpec}, not
   * {@link ComplexFieldSpec.MapFieldSpec}. Returns {@code null} because the types are incompatible.
   */
  @Nullable
  @Override
  public ComplexFieldSpec.MapFieldSpec getFieldSpec() {
    return null;
  }

  /**
   * Returns a per-key {@link DataSource} whose forward index delegates to this column's
   * {@link SparseMapIndexReader} for the requested key.
   *
   * <p>Returns {@code null} if the key is not present in the index (unknown key).
   */
  @Override
  @Nullable
  public DataSource getKeyDataSource(String key) {
    // ConcurrentHashMap.computeIfAbsent does not allow null values, so we cannot
    // cache unknown keys that way. Check the cache first, then build on miss.
    DataSource cached = _keyDataSourceCache.get(key);
    if (cached != null) {
      return cached;
    }
    DataSource ds = buildKeyDataSource(key);
    if (ds == null) {
      return null;
    }
    DataSource existing = _keyDataSourceCache.putIfAbsent(key, ds);
    return existing != null ? existing : ds;
  }

  @Override
  public Map<String, DataSource> getKeyDataSources() {
    Map<String, DataSource> result = new HashMap<>();
    for (String key : _sparseMapIndexReader.getKeys()) {
      DataSource ds = getKeyDataSource(key);
      if (ds != null) {
        result.put(key, ds);
      }
    }
    return result;
  }

  @Override
  public DataSourceMetadata getKeyDataSourceMetadata(String key) {
    throw new UnsupportedOperationException("getKeyDataSourceMetadata not supported for SPARSE_MAP");
  }

  @Override
  public ColumnIndexContainer getKeyIndexContainer(String key) {
    throw new UnsupportedOperationException("getKeyIndexContainer not supported for SPARSE_MAP");
  }

  private DataSource buildKeyDataSource(String key) {
    FieldSpec.DataType keyType = _sparseMapIndexReader.getKeyValueType(key);
    if (keyType == null) {
      return null;
    }

    DimensionFieldSpec keyFieldSpec = new DimensionFieldSpec(key, keyType, true);

    // Try to use pre-built dictionary and dictId reader from immutable reader
    SparseMapKeyDictionary keyDictionary = null;
    FixedBitIntReaderWriter dictIdReader = null;
    if (_sparseMapIndexReader instanceof ImmutableSparseMapIndexReader) {
      ImmutableSparseMapIndexReader immutableReader = (ImmutableSparseMapIndexReader) _sparseMapIndexReader;
      keyDictionary = immutableReader.getKeyDictionary(key);
      dictIdReader = immutableReader.getDictIdReader(key);
    }

    // Fallback: build dictionary from inverted index if pre-built not available
    if (keyDictionary == null) {
      String[] distinctValues = _sparseMapIndexReader.getDistinctValuesForKey(key);
      if (distinctValues != null) {
        String defaultValue = SparseMapKeyDictionary.getDefaultValueString(keyType);
        distinctValues = mergeDefaultValue(distinctValues, defaultValue, keyType);
        keyDictionary = new SparseMapKeyDictionary(keyType, distinctValues);
      }
    }

    SparseMapKeyForwardIndexReader keyReader =
        new SparseMapKeyForwardIndexReader(_sparseMapIndexReader, key, keyType, keyDictionary, dictIdReader,
            _sparseMapIndexReader.getPresenceBitmap(key));

    ColumnIndexContainer.FromMap.Builder containerBuilder =
        new ColumnIndexContainer.FromMap.Builder().with(StandardIndexes.forward(), keyReader);
    if (keyDictionary != null) {
      containerBuilder.with(StandardIndexes.dictionary(), keyDictionary);
    }
    ColumnIndexContainer keyContainer = containerBuilder.build();

    int numDocs = getDataSourceMetadata().getNumDocs();
    int cardinality = keyDictionary != null ? keyDictionary.length() : 0;
    return new BaseDataSource(new KeyDataSourceMetadata(keyFieldSpec, numDocs, cardinality), keyContainer) {
    };
  }

  private static String[] mergeDefaultValue(String[] sortedValues, String defaultValue,
      FieldSpec.DataType keyType) {
    Comparator<String> cmp = SparseMapKeyDictionary.getComparator(keyType);
    int insertionPoint = cmp != null
        ? java.util.Arrays.binarySearch(sortedValues, defaultValue, cmp)
        : java.util.Arrays.binarySearch(sortedValues, defaultValue);
    if (insertionPoint >= 0) {
      // Default value already in the array
      return sortedValues;
    }
    // Insert at the correct position to maintain sorted order
    int pos = -(insertionPoint + 1);
    String[] merged = new String[sortedValues.length + 1];
    System.arraycopy(sortedValues, 0, merged, 0, pos);
    merged[pos] = defaultValue;
    System.arraycopy(sortedValues, pos, merged, pos + 1, sortedValues.length - pos);
    return merged;
  }

  // ---- Per-key DataSourceMetadata ----

  private static class KeyDataSourceMetadata implements DataSourceMetadata {
    private final FieldSpec _keyFieldSpec;
    private final int _numDocs;
    private final int _cardinality;

    KeyDataSourceMetadata(FieldSpec keyFieldSpec, int numDocs, int cardinality) {
      _keyFieldSpec = keyFieldSpec;
      _numDocs = numDocs;
      _cardinality = cardinality;
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _keyFieldSpec;
    }

    @Override
    public boolean isSorted() {
      return false;
    }

    @Override
    public int getNumDocs() {
      return _numDocs;
    }

    @Override
    public int getNumValues() {
      return _numDocs;
    }

    @Override
    public int getMaxNumValuesPerMVEntry() {
      return -1;
    }

    @Nullable
    @Override
    public Comparable getMinValue() {
      return null;
    }

    @Nullable
    @Override
    public Comparable getMaxValue() {
      return null;
    }

    @Nullable
    @Override
    public PartitionFunction getPartitionFunction() {
      return null;
    }

    @Nullable
    @Override
    public Set<Integer> getPartitions() {
      return null;
    }

    @Override
    public int getCardinality() {
      return _cardinality;
    }

    @Override
    public int getMaxRowLengthInBytes() {
      return 0;
    }
  }

  // ---- Metadata for immutable segments ----

  private static class SparseMapDataSourceMetadata implements DataSourceMetadata {
    private final FieldSpec _fieldSpec;
    private final int _numDocs;
    private final int _numValues;
    private final PartitionFunction _partitionFunction;
    private final Set<Integer> _partitions;

    SparseMapDataSourceMetadata(ColumnMetadata columnMetadata) {
      _fieldSpec = columnMetadata.getFieldSpec();
      _numDocs = columnMetadata.getTotalDocs();
      _numValues = columnMetadata.getTotalNumberOfEntries();
      _partitionFunction = columnMetadata.getPartitionFunction();
      _partitions = columnMetadata.getPartitions();
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _fieldSpec;
    }

    @Override
    public boolean isSorted() {
      return false;
    }

    @Override
    public int getNumDocs() {
      return _numDocs;
    }

    @Override
    public int getNumValues() {
      return _numValues;
    }

    @Override
    public int getMaxNumValuesPerMVEntry() {
      return -1;
    }

    @Nullable
    @Override
    public Comparable getMinValue() {
      return null;
    }

    @Nullable
    @Override
    public Comparable getMaxValue() {
      return null;
    }

    @Nullable
    @Override
    public PartitionFunction getPartitionFunction() {
      return _partitionFunction;
    }

    @Nullable
    @Override
    public Set<Integer> getPartitions() {
      return _partitions;
    }

    @Override
    public int getCardinality() {
      return 0;
    }

    @Override
    public int getMaxRowLengthInBytes() {
      return 0;
    }
  }

  // ---- Metadata for mutable (real-time) segments ----

  private static class MutableSparseMapDataSourceMetadata implements DataSourceMetadata {
    private final FieldSpec _fieldSpec;
    private final int _numDocs;

    MutableSparseMapDataSourceMetadata(FieldSpec fieldSpec, int numDocs) {
      _fieldSpec = fieldSpec;
      _numDocs = numDocs;
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _fieldSpec;
    }

    @Override
    public boolean isSorted() {
      return false;
    }

    @Override
    public int getNumDocs() {
      return _numDocs;
    }

    @Override
    public int getNumValues() {
      return _numDocs;
    }

    @Override
    public int getMaxNumValuesPerMVEntry() {
      return -1;
    }

    @Nullable
    @Override
    public Comparable getMinValue() {
      return null;
    }

    @Nullable
    @Override
    public Comparable getMaxValue() {
      return null;
    }

    @Nullable
    @Override
    public PartitionFunction getPartitionFunction() {
      return null;
    }

    @Nullable
    @Override
    public Set<Integer> getPartitions() {
      return null;
    }

    @Override
    public int getCardinality() {
      return 0;
    }

    @Override
    public int getMaxRowLengthInBytes() {
      return 0;
    }
  }
}
