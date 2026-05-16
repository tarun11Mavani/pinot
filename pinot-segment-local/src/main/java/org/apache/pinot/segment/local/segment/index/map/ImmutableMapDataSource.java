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
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;


/// DataSource for immutable MAP columns. Supports two storage backends:
///
/// - **Blob (default):** all keys are read from a single forward index via `MapIndexReader`.
///   Created with the single-arg constructor.
/// - **Columnar (MAP index):** dense keys are materialized as independent virtual columns;
///   sparse keys go into a catch-all JSON column. Created with the overloaded constructor
///   that accepts materialized column DataSources.
///
/// The query layer interacts only with the `MapDataSource` interface and does not need to
/// know which backend is in use.
@SuppressWarnings("rawtypes")
public class ImmutableMapDataSource extends BaseMapDataSource {
  private final MapIndexReader _mapIndexReader;
  private final Map<String, DataSource> _materializedColumnDataSources;
  private final DataSource _sparseDataSource;

  /// Blob-only constructor — no materialized columns.
  public ImmutableMapDataSource(ColumnMetadata columnMetadata, ColumnIndexContainer columnIndexContainer) {
    this(columnMetadata, columnIndexContainer, null, null);
  }

  /// Columnar constructor — with materialized dense-key DataSources and optional sparse column.
  public ImmutableMapDataSource(ColumnMetadata columnMetadata, ColumnIndexContainer indexContainer,
      @Nullable Map<String, DataSource> materializedColumnDataSources, @Nullable DataSource sparseDataSource) {
    super(new ImmutableMapDataSourceMetadata(columnMetadata), indexContainer);
    _materializedColumnDataSources = materializedColumnDataSources;
    _sparseDataSource = sparseDataSource;

    MapIndexReader mapIndexReader;
    ForwardIndexReader<?> forwardIndex = getForwardIndex();
    if (forwardIndex instanceof MapIndexReader) {
      mapIndexReader = (MapIndexReader) forwardIndex;
    } else {
      mapIndexReader = new MapIndexReaderWrapper(forwardIndex, getFieldSpec(), columnMetadata.getTotalDocs());
    }
    _mapIndexReader = mapIndexReader;

    if (materializedColumnDataSources != null) {
      _keyDataSources.putAll(materializedColumnDataSources);
    }
  }

  @Override
  public MapIndexReader<ForwardIndexReaderContext> getMapIndexReader() {
    return _mapIndexReader;
  }

  @Override
  public boolean containsKey(String key) {
    if (_materializedColumnDataSources != null) {
      if (_materializedColumnDataSources.containsKey(key)) {
        return true;
      }
      return _sparseDataSource != null;
    }
    return true;
  }

  @Override
  public Map<String, DataSource> getDataSources() {
    if (_materializedColumnDataSources != null) {
      return new HashMap<>(_materializedColumnDataSources);
    }
    return super.getDataSources();
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata(String key) {
    if (_materializedColumnDataSources != null) {
      DataSource ds = _materializedColumnDataSources.get(key);
      if (ds != null) {
        return ds.getDataSourceMetadata();
      }
    }
    return null;
  }

  @Override
  public ColumnIndexContainer getIndexContainer(String key) {
    return null;
  }

  static class ImmutableMapDataSourceMetadata implements DataSourceMetadata {
    final FieldSpec _fieldSpec;
    final int _numDocs;
    final int _numValues;
    final int _maxNumValuesPerMVEntry;
    final int _cardinality;
    final PartitionFunction _partitionFunction;
    final Set<Integer> _partitions;
    final Comparable _minValue;
    final Comparable _maxValue;

    ImmutableMapDataSourceMetadata(ColumnMetadata columnMetadata) {
      _fieldSpec = columnMetadata.getFieldSpec();
      _numDocs = columnMetadata.getTotalDocs();
      _numValues = columnMetadata.getTotalNumberOfEntries();
      if (_fieldSpec.isSingleValueField()) {
        _maxNumValuesPerMVEntry = -1;
      } else {
        _maxNumValuesPerMVEntry = columnMetadata.getMaxNumberOfMultiValues();
      }
      _minValue = columnMetadata.getMinValue();
      _maxValue = columnMetadata.getMaxValue();
      _partitionFunction = columnMetadata.getPartitionFunction();
      _partitions = columnMetadata.getPartitions();
      _cardinality = columnMetadata.getCardinality();
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
      throw new UnsupportedOperationException();
    }
  }
}
