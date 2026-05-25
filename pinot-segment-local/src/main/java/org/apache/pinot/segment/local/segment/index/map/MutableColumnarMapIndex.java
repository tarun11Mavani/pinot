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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.segment.local.realtime.impl.dictionary.MutableDictionaryFactory;
import org.apache.pinot.segment.local.realtime.impl.forward.FixedByteSVMutableForwardIndex;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeInvertedIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableDictionary;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.reader.ColumnarMapIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages per-key mutable columns for a MAP column with MAP index enabled.
 * Each discovered key gets its own mutable forward index and presence bitmap.
 * All keys are treated as dense during the consuming phase; tier split happens at seal time.
 *
 * Single-writer for index() (consuming thread). Multi-reader for query-side methods.
 * Readers see consistent state via volatile reference swap.
 */
public class MutableColumnarMapIndex implements ColumnarMapIndexReader, Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutableColumnarMapIndex.class);
  private static final int DEFAULT_AVG_STRING_LENGTH = 32;
  private static final int DEFAULT_ROWS_PER_CHUNK = 1000;

  private final String _mapColumn;
  private final MapIndexConfig _config;
  private final Map<String, FieldSpec> _valueFieldSpecs;
  private final DataType _defaultValueType;
  private final int _maxDenseKeys;
  private final PinotDataBufferMemoryManager _memoryManager;
  private final int _capacity;

  private volatile Map<String, MutableKeyColumn> _keyColumns = new HashMap<>();
  private int _distinctKeyCount = 0;
  private final Set<String> _droppedKeys = new HashSet<>();

  public MutableColumnarMapIndex(String mapColumn, ComplexFieldSpec fieldSpec, MapIndexConfig config,
      PinotDataBufferMemoryManager memoryManager, int capacity) {
    _mapColumn = mapColumn;
    _config = config;
    _maxDenseKeys = config.getMaxDenseKeys();
    _memoryManager = memoryManager;
    _capacity = capacity;

    Map<String, FieldSpec> valueFieldSpecs = fieldSpec.getValueFieldSpecs();
    _valueFieldSpecs = valueFieldSpecs != null ? new HashMap<>(valueFieldSpecs) : new HashMap<>();
    FieldSpec defaultSpec = fieldSpec.getDefaultValueFieldSpec();
    _defaultValueType = defaultSpec != null ? defaultSpec.getDataType() : DataType.STRING;
  }

  @SuppressWarnings("unchecked")
  public void index(int docId, @Nullable Object value) {
    if (!(value instanceof Map)) {
      return;
    }
    Map<String, Object> map = (Map<String, Object>) value;
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      String key = entry.getKey();
      Object rawValue = entry.getValue();
      if (rawValue == null) {
        continue;
      }

      MutableKeyColumn keyCol = _keyColumns.get(key);
      if (keyCol == null) {
        if (_distinctKeyCount >= _maxDenseKeys) {
          if (_droppedKeys.add(key)) {
            LOGGER.warn("MAP for '{}' reached maxDenseKeys ({}).. Dropping '{}'.",
                _mapColumn, _maxDenseKeys, key);
          }
          continue;
        }
        keyCol = getOrCreateKeyColumn(key);
      }

      DataType storedType = keyCol.getStoredType();
      Object coerced;
      try {
        PinotDataType sourceType = PinotDataType.getSingleValueType(rawValue.getClass());
        PinotDataType destType = PinotDataType.getPinotDataTypeForExecution(
            ColumnDataType.fromDataTypeSV(storedType));
        coerced = destType.convert(rawValue, sourceType);
      } catch (Exception e) {
        LOGGER.warn("MAP '{}': coercion failed for key '{}' to {}. Skipping.",
            _mapColumn, key, storedType, e);
        continue;
      }

      keyCol.index(docId, coerced);
    }
  }

  // TODO(C2): _distinctKeyCount is incremented before coercion — a first-row coercion failure
  // permanently consumes a maxDenseKeys slot. Move increment to after successful coercion.
  private MutableKeyColumn getOrCreateKeyColumn(String key) {
    MutableKeyColumn existing = _keyColumns.get(key);
    if (existing != null) {
      return existing;
    }
    _distinctKeyCount++;
    FieldSpec spec = _valueFieldSpecs.get(key);
    DataType valueType = spec != null ? spec.getDataType() : _defaultValueType;
    DataType storedType = valueType.getStoredType();
    String allocationContext = _mapColumn + "$" + key;
    MutableKeyColumn newCol = new MutableKeyColumn(key, storedType, _memoryManager, _capacity, allocationContext);
    Map<String, MutableKeyColumn> updated = new HashMap<>(_keyColumns);
    updated.put(key, newCol);
    _keyColumns = updated;
    return newCol;
  }

  public Map<String, MutableKeyColumn> getKeyColumns() {
    return _keyColumns;
  }

  @Override
  public Set<String> getKeys() {
    return _keyColumns.keySet();
  }

  @Nullable
  public MutableKeyColumn getKeyColumn(String key) {
    return _keyColumns.get(key);
  }

  @Override
  @Nullable
  public DataType getValueType(String key) {
    if (!_keyColumns.containsKey(key) && !_valueFieldSpecs.containsKey(key)) {
      return null;
    }
    FieldSpec spec = _valueFieldSpecs.get(key);
    DataType type = spec != null ? spec.getDataType() : _defaultValueType;
    return type.getStoredType();
  }

  @Override
  public int getNumDocsWithKey(String key) {
    MutableKeyColumn col = _keyColumns.get(key);
    return col == null ? 0 : col.getPresenceBitmap().getCardinality();
  }

  @Override
  public ImmutableRoaringBitmap getPresenceBitmap(String key) {
    MutableKeyColumn col = _keyColumns.get(key);
    return col == null ? new MutableRoaringBitmap() : col.getPresenceBitmap();
  }

  @Override
  public Map<String, Object> getMap(int docId) {
    Map<String, Object> result = new HashMap<>();
    for (Map.Entry<String, MutableKeyColumn> entry : _keyColumns.entrySet()) {
      MutableKeyColumn col = entry.getValue();
      if (col.getPresenceBitmap().contains(docId)) {
        result.put(entry.getKey(), col.getValue(docId));
      }
    }
    return result;
  }

  @Override
  public void close()
      throws IOException {
    for (MutableKeyColumn keyCol : _keyColumns.values()) {
      keyCol.close();
    }
  }

  /**
   * A single key's mutable column: forward index + presence bitmap.
   */
  public static class MutableKeyColumn implements Closeable {
    private final String _key;
    private final DataType _storedType;
    private final MutableForwardIndex _forwardIndex;
    private final MutableRoaringBitmap _presenceBitmap;
    private final MutableDictionary _dictionary;
    private final RealtimeInvertedIndex _invertedIndex;

    MutableKeyColumn(String key, DataType storedType, PinotDataBufferMemoryManager memoryManager,
        int capacity, String allocationContext) {
      _key = key;
      _storedType = storedType;
      _presenceBitmap = new MutableRoaringBitmap();
      _invertedIndex = new RealtimeInvertedIndex();

      int estimatedCardinality = Math.max(capacity / 100, 16);
      int avgLength = storedType.isFixedWidth() ? storedType.size() : DEFAULT_AVG_STRING_LENGTH;
      _dictionary = MutableDictionaryFactory.getMutableDictionary(
          storedType, false, memoryManager, avgLength, estimatedCardinality,
          allocationContext + ".dict");

      _forwardIndex = new FixedByteSVMutableForwardIndex(true, DataType.INT,
          DEFAULT_ROWS_PER_CHUNK, memoryManager, allocationContext + ".fwd");
    }

    public String getKey() {
      return _key;
    }

    public DataType getStoredType() {
      return _storedType;
    }

    public MutableForwardIndex getForwardIndex() {
      return _forwardIndex;
    }

    public MutableRoaringBitmap getPresenceBitmap() {
      return _presenceBitmap;
    }

    public void index(int docId, Object value) {
      _presenceBitmap.add(docId);
      int dictId = _dictionary.index(value);
      _forwardIndex.setDictId(docId, dictId);
      _invertedIndex.add(dictId, docId);
    }

    public Object getValue(int docId) {
      int dictId = _forwardIndex.getDictId(docId, null);
      if (dictId < 0 || dictId >= _dictionary.length()) {
        return null;
      }
      return _dictionary.get(dictId);
    }

    public MutableDictionary getDictionary() {
      return _dictionary;
    }

    public RealtimeInvertedIndex getInvertedIndex() {
      return _invertedIndex;
    }

    @Override
    public void close()
        throws IOException {
      _forwardIndex.close();
      _dictionary.close();
      _invertedIndex.close();
    }
  }
}
