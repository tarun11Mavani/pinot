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

package org.apache.pinot.segment.local.segment.index.map;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.nullvalue.MutableNullValueVector;
import org.apache.pinot.segment.local.segment.index.datasource.ImmutableDataSource;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.reader.ColumnarMapIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MapNaming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// DataSource for MAP columns in mutable (consuming) segments. Supports two storage backends:
///
/// - **Blob (default):** all keys are read from a single forward index via `MapIndexReader`.
///   Created with the blob-only constructor.
/// - **Columnar (MAP index):** per-key lookups are routed to `MutableColumnarMapIndex`.
///   Created with the overloaded constructor that accepts a `MutableColumnarMapIndex`.
@SuppressWarnings("rawtypes")
public class MutableMapDataSource extends BaseMapDataSource {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutableMapDataSource.class);

  private final MapIndexReader _mapIndexReader;
  private final MutableColumnarMapIndex _columnarMapIndex;
  private final int _numDocs;

  /// Blob-only constructor — no columnar MAP index.
  public MutableMapDataSource(FieldSpec fieldSpec, int numDocs, int numValues, int maxNumValuesPerMVEntry,
      int cardinality, @Nullable PartitionFunction partitionFunction, @Nullable Set<Integer> partitions,
      @Nullable Comparable minValue, @Nullable Comparable maxValue, Map<IndexType, MutableIndex> mutableIndexes,
      @Nullable MutableDictionary dictionary, @Nullable MutableNullValueVector nullValueVector,
      int maxRowLengthInBytes) {
    super(new MutableMapDataSourceMetadata(fieldSpec, numDocs, numValues, maxNumValuesPerMVEntry, cardinality,
            partitionFunction, partitions, minValue, maxValue, maxRowLengthInBytes),
        new ColumnIndexContainer.FromMap.Builder().withAll(mutableIndexes).build());
    _columnarMapIndex = null;
    _numDocs = numDocs;
    _mapIndexReader = resolveMapIndexReader(getForwardIndex(), getFieldSpec(), numDocs);
  }

  /// Columnar constructor — with per-key `MutableColumnarMapIndex`.
  public MutableMapDataSource(FieldSpec fieldSpec, int numDocs, int numValues,
      int maxNumValuesPerMVEntry, int cardinality,
      @Nullable PartitionFunction partitionFunction, @Nullable Set<Integer> partitions,
      @Nullable Comparable minValue, @Nullable Comparable maxValue,
      MutableColumnarMapIndex columnarMapIndex, ColumnIndexContainer indexContainer,
      int maxRowLengthInBytes) {
    super(new MutableMapDataSourceMetadata(fieldSpec, numDocs, numValues, maxNumValuesPerMVEntry, cardinality,
            partitionFunction, partitions, minValue, maxValue, maxRowLengthInBytes),
        indexContainer);
    _columnarMapIndex = columnarMapIndex;
    _numDocs = numDocs;
    _mapIndexReader = null;
  }

  private static MapIndexReader resolveMapIndexReader(
      ForwardIndexReader<?> forwardIndex, FieldSpec fieldSpec, int numDocs) {
    if (forwardIndex instanceof MapIndexReader) {
      return (MapIndexReader) forwardIndex;
    }
    return new MapIndexReaderWrapper(forwardIndex, fieldSpec, numDocs);
  }

  @Override
  public MapIndexReader<ForwardIndexReaderContext> getMapIndexReader() {
    return _mapIndexReader;
  }

  public boolean hasColumnarMapIndex() {
    return _columnarMapIndex != null;
  }

  @Nullable
  public ColumnarMapIndexReader getColumnarMapReader() {
    return _columnarMapIndex;
  }

  @Override
  public boolean containsKey(String key) {
    if (_columnarMapIndex != null) {
      return _columnarMapIndex.getKeys().contains(key);
    }
    return true;
  }

  @Override
  public DataSource getDataSource(String key) {
    if (_columnarMapIndex == null) {
      return super.getDataSource(key);
    }
    MutableColumnarMapIndex.MutableKeyColumn keyCol = _columnarMapIndex.getKeyColumn(key);
    if (keyCol == null) {
      return new NullDataSource(key);
    }

    DataType storedType = keyCol.getStoredType();
    String materializedCol = MapNaming.materializedColumnName(
        getDataSourceMetadata().getFieldSpec().getName(), key);
    FieldSpec keyFieldSpec = new DimensionFieldSpec(materializedCol, storedType, true);

    ColumnMetadata keyMeta = new ColumnMetadataImpl.Builder()
        .setFieldSpec(keyFieldSpec)
        .setTotalDocs(_numDocs)
        .setCardinality(keyCol.getDictionary().length())
        .setHasDictionary(true)
        .setSorted(false)
        .build();

    MutableForwardIndex fwdIndex = keyCol.getForwardIndex();
    ColumnIndexContainer keyIndexContainer = new ColumnIndexContainer.FromMap.Builder()
        .with(StandardIndexes.forward(), fwdIndex)
        .with(StandardIndexes.dictionary(), keyCol.getDictionary())
        .with(StandardIndexes.inverted(), keyCol.getInvertedIndex())
        .build();

    return new ImmutableDataSource(keyMeta, keyIndexContainer);
  }

  @Override
  public Map<String, DataSource> getDataSources() {
    if (_columnarMapIndex == null) {
      return super.getDataSources();
    }
    Map<String, DataSource> all = new HashMap<>();
    for (String key : _columnarMapIndex.getKeys()) {
      all.put(key, getDataSource(key));
    }
    return all;
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata(String key) {
    if (_columnarMapIndex == null) {
      return super.getDataSourceMetadata(key);
    }
    DataSource ds = getDataSource(key);
    return ds != null ? ds.getDataSourceMetadata() : null;
  }

  @Override
  public ColumnIndexContainer getIndexContainer(String key) {
    if (_columnarMapIndex == null) {
      return super.getIndexContainer(key);
    }
    MutableColumnarMapIndex.MutableKeyColumn keyCol = _columnarMapIndex.getKeyColumn(key);
    if (keyCol == null) {
      return null;
    }
    return new ColumnIndexContainer.FromMap.Builder()
        .with(StandardIndexes.forward(), keyCol.getForwardIndex())
        .with(StandardIndexes.dictionary(), keyCol.getDictionary())
        .with(StandardIndexes.inverted(), keyCol.getInvertedIndex())
        .build();
  }

  static class MutableMapDataSourceMetadata implements DataSourceMetadata {
    final FieldSpec _fieldSpec;
    final int _numDocs;
    final int _numValues;
    final int _maxNumValuesPerMVEntry;
    final int _cardinality;
    final PartitionFunction _partitionFunction;
    final Set<Integer> _partitions;
    final Comparable _minValue;
    final Comparable _maxValue;
    final int _maxRowLengthInBytes;

    MutableMapDataSourceMetadata(FieldSpec fieldSpec, int numDocs, int numValues, int maxNumValuesPerMVEntry,
        int cardinality, @Nullable PartitionFunction partitionFunction, @Nullable Set<Integer> partitions,
        @Nullable Comparable minValue, @Nullable Comparable maxValue, int maxRowLengthInBytes) {
      _fieldSpec = fieldSpec;
      _numDocs = numDocs;
      _numValues = numValues;
      _maxNumValuesPerMVEntry = maxNumValuesPerMVEntry;
      if (partitionFunction != null) {
        _partitionFunction = partitionFunction;
        _partitions = partitions;
      } else {
        _partitionFunction = null;
        _partitions = null;
      }
      _minValue = minValue;
      _maxValue = maxValue;
      _cardinality = cardinality;
      _maxRowLengthInBytes = maxRowLengthInBytes;
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _fieldSpec;
    }

    @Override
    public boolean isSorted() {
      // NOTE: Mutable data source is never sorted
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
      return _maxNumValuesPerMVEntry;
    }

    @Nullable
    @Override
    public Comparable getMinValue() {
      return _minValue;
    }

    @Override
    public Comparable getMaxValue() {
      return _maxValue;
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
      return _cardinality;
    }

    @Override
    public int getMaxRowLengthInBytes() {
      return _maxRowLengthInBytes;
    }
  }
}
