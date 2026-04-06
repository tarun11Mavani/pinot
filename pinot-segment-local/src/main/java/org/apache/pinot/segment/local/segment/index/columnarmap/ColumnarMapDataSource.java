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
package org.apache.pinot.segment.local.segment.index.columnarmap;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.local.segment.index.datasource.BaseDataSource;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.MapDataSource;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.ColumnarMapIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Data source for MAP columns with sparse map index. Wraps a {@link ColumnarMapIndexReader} and exposes it for
 * query access.
 *
 * <p>Implements {@link MapDataSource} so that {@code metrics['key']} expressions handled by
 * {@code ItemTransformFunction} can resolve per-key {@link DataSource} instances via
 * {@link #getKeyDataSource(String)}.  Each per-key data source wraps a
 * {@link ColumnarMapKeyForwardIndexReader} that delegates typed reads directly to the underlying
 * {@link ColumnarMapIndexReader}.
 */
public class ColumnarMapDataSource extends BaseDataSource implements MapDataSource, Closeable {

  private static final double DENSE_KEY_FILL_RATE_THRESHOLD = 0.05;

  private final ColumnarMapIndexReader _columnarMapIndexReader;
  private final Map<String, DataSource> _keyDataSourceCache = new ConcurrentHashMap<>();
  private final List<PinotDataBuffer> _expandedBuffers = new CopyOnWriteArrayList<>();

  /**
   * Constructs a ColumnarMapDataSource for an immutable segment.
   */
  public ColumnarMapDataSource(ColumnMetadata columnMetadata, ColumnarMapIndexReader columnarMapIndexReader) {
    super(new ColumnarMapDataSourceMetadata(columnMetadata),
        buildContainerWithForwardIndex(columnarMapIndexReader));
    _columnarMapIndexReader = columnarMapIndexReader;
  }

  /**
   * Constructs a ColumnarMapDataSource for a mutable (real-time) segment.
   */
  public ColumnarMapDataSource(FieldSpec fieldSpec, int numDocs, ColumnarMapIndexReader columnarMapIndexReader) {
    super(new MutableColumnarMapDataSourceMetadata(fieldSpec, numDocs),
        buildContainerWithForwardIndex(columnarMapIndexReader));
    _columnarMapIndexReader = columnarMapIndexReader;
  }

  private static ColumnIndexContainer buildContainerWithForwardIndex(ColumnarMapIndexReader reader) {
    return new ColumnIndexContainer.FromMap.Builder()
        .with(StandardIndexes.forward(), new ColumnarMapForwardIndexReader(reader))
        .build();
  }

  /**
   * Returns the ColumnarMapIndexReader for this column.
   */
  public ColumnarMapIndexReader getColumnarMapIndexReader() {
    return _columnarMapIndexReader;
  }

  // ---- MapDataSource implementation ----

  /**
   * MAP columns with sparse map index use per-key typed storage rather than the homogeneous
   * {@link ComplexFieldSpec.MapFieldSpec}. Returns {@code null} because the types are incompatible.
   */
  @Nullable
  @Override
  public ComplexFieldSpec.MapFieldSpec getFieldSpec() {
    return null;
  }

  /**
   * Returns a per-key {@link DataSource} whose forward index delegates to this column's
   * {@link ColumnarMapIndexReader} for the requested key.
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
    for (String key : _columnarMapIndexReader.getKeys()) {
      DataSource ds = getKeyDataSource(key);
      if (ds != null) {
        result.put(key, ds);
      }
    }
    return result;
  }

  @Override
  public DataSourceMetadata getKeyDataSourceMetadata(String key) {
    throw new UnsupportedOperationException("getKeyDataSourceMetadata not supported for MAP with sparse map index");
  }

  @Override
  public ColumnIndexContainer getKeyIndexContainer(String key) {
    throw new UnsupportedOperationException("getKeyIndexContainer not supported for MAP with sparse map index");
  }

  private DataSource buildKeyDataSource(String key) {
    FieldSpec.DataType keyType = _columnarMapIndexReader.getKeyValueType(key);
    if (keyType == null) {
      return null;
    }

    DimensionFieldSpec keyFieldSpec = new DimensionFieldSpec(key, keyType, true);

    // Try to use pre-built dictionary and dictId reader from immutable reader
    ColumnarMapKeyDictionary keyDictionary = null;
    FixedBitIntReaderWriter dictIdReader = null;
    if (_columnarMapIndexReader instanceof ImmutableColumnarMapIndexReader) {
      ImmutableColumnarMapIndexReader immutableReader = (ImmutableColumnarMapIndexReader) _columnarMapIndexReader;
      keyDictionary = immutableReader.getKeyDictionary(key);
      dictIdReader = immutableReader.getDictIdReader(key);
    }

    // Fallback: build dictionary from inverted index if pre-built not available
    if (keyDictionary == null) {
      String[] distinctValues = _columnarMapIndexReader.getDistinctValuesForKey(key);
      if (distinctValues != null) {
        String defaultValue = ColumnarMapKeyDictionary.getDefaultValueString(keyType);
        distinctValues = mergeDefaultValue(distinctValues, defaultValue, keyType);
        keyDictionary = new ColumnarMapKeyDictionary(keyType, distinctValues);
      }
    }

    // For dense keys, expand ordinal-indexed dictId array to docId-indexed for O(1) reads
    ImmutableRoaringBitmap presenceBitmap = _columnarMapIndexReader.getPresenceBitmap(key);
    FixedBitIntReaderWriter readerForFwdIndex = dictIdReader;
    ImmutableRoaringBitmap bitmapForFwdIndex = presenceBitmap;

    if (dictIdReader != null && keyDictionary != null && presenceBitmap != null) {
      int numDocs = getDataSourceMetadata().getNumDocs();
      long presentCount = presenceBitmap.getLongCardinality();
      double fillRate = numDocs > 0 ? (double) presentCount / numDocs : 0;

      if (fillRate >= DENSE_KEY_FILL_RATE_THRESHOLD) {
        int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(Math.max(keyDictionary.length() - 1, 0));
        long bufferSize = ((long) numDocs * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
        PinotDataBuffer expandedBuffer = PinotDataBuffer.allocateDirect(
            bufferSize, java.nio.ByteOrder.BIG_ENDIAN, "expanded-dictId-" + key);
        _expandedBuffers.add(expandedBuffer);

        FixedBitIntReaderWriter expandedReader = new FixedBitIntReaderWriter(expandedBuffer, numDocs, numBitsPerValue);

        // Fill with default dictId for absent docs
        String defaultValueStr = ColumnarMapKeyDictionary.getDefaultValueString(keyType);
        int defaultDictId = keyDictionary.indexOf(defaultValueStr);
        for (int docId = 0; docId < numDocs; docId++) {
          expandedReader.writeInt(docId, defaultDictId);
        }

        // Overwrite present docs with actual dictIds from ordinal-indexed reader
        PeekableIntIterator iter = presenceBitmap.getIntIterator();
        int ordinal = 0;
        while (iter.hasNext()) {
          int docId = iter.next();
          expandedReader.writeInt(docId, dictIdReader.readInt(ordinal));
          ordinal++;
        }

        readerForFwdIndex = expandedReader;
        bitmapForFwdIndex = null;  // no bitmap needed for docId-indexed reader
      }
    }

    ColumnarMapKeyForwardIndexReader keyReader =
        new ColumnarMapKeyForwardIndexReader(_columnarMapIndexReader, key, keyType, keyDictionary, readerForFwdIndex,
            bitmapForFwdIndex);

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
    Comparator<String> cmp = ColumnarMapKeyDictionary.getComparator(keyType);
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

  private static class ColumnarMapDataSourceMetadata implements DataSourceMetadata {
    private final FieldSpec _fieldSpec;
    private final int _numDocs;
    private final int _numValues;
    private final PartitionFunction _partitionFunction;
    private final Set<Integer> _partitions;

    ColumnarMapDataSourceMetadata(ColumnMetadata columnMetadata) {
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

  private static class MutableColumnarMapDataSourceMetadata implements DataSourceMetadata {
    private final FieldSpec _fieldSpec;
    private final int _numDocs;

    MutableColumnarMapDataSourceMetadata(FieldSpec fieldSpec, int numDocs) {
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

  @Override
  public void close()
      throws IOException {
    for (PinotDataBuffer buffer : _expandedBuffers) {
      buffer.close();
    }
    _expandedBuffers.clear();
  }
}
