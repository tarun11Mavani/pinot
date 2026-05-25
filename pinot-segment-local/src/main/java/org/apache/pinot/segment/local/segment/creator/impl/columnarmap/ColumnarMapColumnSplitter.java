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
package org.apache.pinot.segment.local.segment.creator.impl.columnarmap;

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.PinotDataType;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.local.io.writer.impl.FixedBitSVForwardIndexWriter;
import org.apache.pinot.segment.local.segment.creator.impl.BaseSegmentCreator;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.nullvalue.NullValueVectorCreator;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.creator.ColumnarMapIndexCreator;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MapNaming;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.JsonUtils;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Splits a MAP column into per-key virtual columns using standard Pinot index creators.
 * Dense keys become independent columns in index_map; sparse keys go into a synthetic JSON column.
 *
 * <p>Lifecycle: instantiated by BaseSegmentCreator for MAP columns with MAP enabled.
 * Receives per-doc Map values via {@link #add(Object, int)}, accumulates in memory, then on
 * {@link #seal()} writes per-key column files using standard creators.
 */
public class ColumnarMapColumnSplitter implements ColumnarMapIndexCreator {

  private static final Logger LOGGER = LoggerFactory.getLogger(ColumnarMapColumnSplitter.class);
  private static final double NO_DICTIONARY_SIZE_RATIO_THRESHOLD = 0.85;

  private final File _indexDir;
  private final String _columnName;
  private final Map<String, FieldSpec> _valueFieldSpecs;
  private final DataType _defaultValueType;
  private final MapIndexConfig _config;
  private final int _maxDenseKeys;

  // Per-key accumulation
  private final Map<String, RoaringBitmap> _presenceBitmaps = new HashMap<>();
  private final Map<String, List<Object>> _values = new HashMap<>();
  private final Map<String, Set<String>> _distinctValuesPerKey = new HashMap<>();
  private final Map<String, Long> _totalRawBytesPerKey = new HashMap<>();
  private int _numDocs;
  private int _distinctKeyCount;

  // Resolved at seal time
  private Set<String> _resolvedDenseKeys;
  private final Map<String, PropertiesConfiguration> _materializedColumnMetadata = new LinkedHashMap<>();

  public ColumnarMapColumnSplitter(File indexDir, String columnName, FieldSpec fieldSpec,
      MapIndexConfig config) {
    _indexDir = indexDir;
    _columnName = columnName;
    _config = config;
    _maxDenseKeys = config.getMaxDenseKeys();

    Map<String, FieldSpec> valueFieldSpecs = null;
    DataType defaultType = null;
    if (fieldSpec instanceof ComplexFieldSpec) {
      ComplexFieldSpec complexSpec = (ComplexFieldSpec) fieldSpec;
      valueFieldSpecs = complexSpec.getValueFieldSpecs();
      FieldSpec defaultSpec = complexSpec.getDefaultValueFieldSpec();
      defaultType = defaultSpec != null ? defaultSpec.getDataType() : null;
    }
    _valueFieldSpecs = valueFieldSpecs != null ? new HashMap<>(valueFieldSpecs) : new HashMap<>();
    _defaultValueType = defaultType != null ? defaultType : DataType.STRING;
  }

  @Override
  public void add(Object value, int dictId)
      throws IOException {
    if (value instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) value;
      addMap(map);
    } else {
      addMap(null);
    }
  }

  @Override
  public void add(Map<String, Object> mapValue, int docId)
      throws IOException {
    add((Object) mapValue, docId);
  }

  @Override
  public void add(Object[] values, @Nullable int[] dictIds)
      throws IOException {
    throw new UnsupportedOperationException("MAP with MAP index is single-value only");
  }

  private void addMap(@Nullable Map<String, Object> map) {
    if (map != null && !map.isEmpty()) {
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        String key = entry.getKey();
        Object rawValue = entry.getValue();
        if (rawValue == null) {
          continue;
        }
        FieldSpec keySpec = _valueFieldSpecs.get(key);
        DataType valueType = keySpec != null ? keySpec.getDataType() : _defaultValueType;
        if (!_presenceBitmaps.containsKey(key)) {
          _presenceBitmaps.put(key, new RoaringBitmap());
          _values.put(key, new ArrayList<>());
          _distinctKeyCount++;
        }
        _presenceBitmaps.get(key).add(_numDocs);
        Object coerced;
        try {
          PinotDataType sourceType = PinotDataType.getSingleValueType(rawValue.getClass());
          PinotDataType destType = PinotDataType.getPinotDataTypeForExecution(
              ColumnDataType.fromDataTypeSV(valueType.getStoredType()));
          coerced = destType.convert(rawValue, sourceType);
        } catch (Exception e) {
          LOGGER.warn("MAP '{}': coercion failed for key '{}' value '{}' to {}. Skipping.",
              _columnName, key, rawValue, valueType, e);
          _presenceBitmaps.get(key).remove(_numDocs);
          continue;
        }
        _values.get(key).add(coerced);

        DataType storedType = valueType.getStoredType();
        String stringRep = storedType.toString(coerced);
        _distinctValuesPerKey.computeIfAbsent(key, k -> new HashSet<>()).add(stringRep);
        if (storedType == DataType.STRING || storedType == DataType.BYTES) {
          byte[] rawBytes = storedType == DataType.BYTES ? (byte[]) coerced
              : ((String) coerced).getBytes(StandardCharsets.UTF_8);
          _totalRawBytesPerKey.merge(key, (long) rawBytes.length, Long::sum);
        }
      }
    }
    _numDocs++;
  }

  @Override
  public void seal()
      throws IOException {
    if (_numDocs == 0 || _presenceBitmaps.isEmpty()) {
      _resolvedDenseKeys = new LinkedHashSet<>();
      return;
    }
    List<String> allKeys = new ArrayList<>(_presenceBitmaps.keySet());

    // Rank keys by fill rate (descending), then by key name (ascending) for deterministic tiebreak
    allKeys.sort((a, b) -> {
      double fillA = (double) _presenceBitmaps.get(a).getCardinality() / _numDocs;
      double fillB = (double) _presenceBitmaps.get(b).getCardinality() / _numDocs;
      int cmp = Double.compare(fillB, fillA); // descending
      return cmp != 0 ? cmp : a.compareTo(b); // lex tiebreak ascending
    });

    // Top maxDenseKeys by fillRate -> dense (also respect denseKeyMinFillRate threshold)
    double minFillRate = _config.getDenseKeyMinFillRate();
    _resolvedDenseKeys = new LinkedHashSet<>();
    List<String> sparseKeys = new ArrayList<>();

    // Always include explicitly configured dense keys first
    Set<String> configuredDenseKeys = _config.getDenseKeys();
    for (String key : configuredDenseKeys) {
      if (_presenceBitmaps.containsKey(key) && _resolvedDenseKeys.size() < _maxDenseKeys) {
        _resolvedDenseKeys.add(key);
      }
    }

    for (String key : allKeys) {
      if (_resolvedDenseKeys.contains(key)) {
        continue; // already added as configured dense key
      }
      double fillRate = (double) _presenceBitmaps.get(key).getCardinality() / _numDocs;
      if (_resolvedDenseKeys.size() < _maxDenseKeys && fillRate >= minFillRate) {
        _resolvedDenseKeys.add(key);
      } else {
        sparseKeys.add(key);
      }
    }

    // Write dense virtual columns
    for (String key : _resolvedDenseKeys) {
      writeDenseKeyColumn(key);
    }

    // Write sparse JSON column for remaining keys
    if (!sparseKeys.isEmpty()) {
      writeSparseJsonColumn(sparseKeys);
    }
  }

  @Override
  public void close()
      throws IOException {
    // Nothing to close — all sub-creators are created and closed within seal()
  }

  /**
   * Returns metadata properties for all virtual columns created by this splitter.
   * Call after seal().
   */
  @Override
  public Map<String, PropertiesConfiguration> getMaterializedColumnMetadata() {
    return _materializedColumnMetadata;
  }

  /**
   * Returns the set of dense keys resolved at seal time.
   */
  public Set<String> getResolvedDenseKeys() {
    return _resolvedDenseKeys != null ? _resolvedDenseKeys : Set.of();
  }

  private void writeDenseKeyColumn(String key)
      throws IOException {
    String materializedCol = MapNaming.materializedColumnName(_columnName, key);
    FieldSpec keySpec = _valueFieldSpecs.get(key);
    DataType valueType = keySpec != null ? keySpec.getDataType() : _defaultValueType;
    DataType storedType = valueType.getStoredType();
    RoaringBitmap presence = _presenceBitmaps.get(key);
    List<Object> values = _values.get(key);
    int numDocsForKey = presence.getCardinality();

    boolean useDictionary = shouldUseDictionary(key, storedType, numDocsForKey);
    boolean enableInverted = _config.shouldEnableInvertedIndexForKey(key);

    // Build sorted distinct values for dictionary
    Object sortedDistinctArray = null;
    int cardinality = 0;
    if (useDictionary) {
      sortedDistinctArray = buildTypedDistinctArray(key, storedType);
      cardinality = java.lang.reflect.Array.getLength(sortedDistinctArray);
    } else {
      cardinality = numDocsForKey;
    }

    // Min/max computed from actual typed values. Wrap byte[] in ByteArray since byte[] is not Comparable.
    Comparable minValue = null;
    Comparable maxValue = null;
    for (Object v : values) {
      Comparable cv = (storedType == DataType.BYTES) ? new ByteArray((byte[]) v) : (Comparable) v;
      if (minValue == null || cv.compareTo(minValue) < 0) {
        minValue = cv;
      }
      if (maxValue == null || cv.compareTo(maxValue) > 0) {
        maxValue = cv;
      }
    }

    // 1. Write dictionary (if applicable) — keep dictCreator open for indexOfSV lookups
    SegmentDictionaryCreator dictCreator = null;
    if (useDictionary) {
      dictCreator = new SegmentDictionaryCreator(
          materializedCol, storedType, new File(_indexDir,
          materializedCol + V1Constants.Dict.FILE_EXTENSION), true);
      dictCreator.build(sortedDistinctArray);
    }

    try {
      // 2. Write forward index
      if (useDictionary) {
        int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(Math.max(cardinality - 1, 0));
        int defaultDictId = dictCreator.indexOfSV(getDefaultValue(storedType));

        FixedBitSVForwardIndexWriter fwdWriter = new FixedBitSVForwardIndexWriter(
            new File(_indexDir, materializedCol + V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION),
            _numDocs, numBitsPerValue);
        try {
          int ordinal = 0;
          for (int docId = 0; docId < _numDocs; docId++) {
            if (presence.contains(docId)) {
              Object typedValue = values.get(ordinal++);
              fwdWriter.putDictId(dictCreator.indexOfSV(typedValue));
            } else {
              fwdWriter.putDictId(defaultDictId);
            }
          }
        } finally {
          fwdWriter.close();
        }
      } else {
        writeRawForwardIndex(materializedCol, storedType, presence, values);
      }

      // 3. Write inverted index (if applicable)
      if (enableInverted && useDictionary) {
        FieldSpec fakeFieldSpec = new DimensionFieldSpec(materializedCol, storedType, true);
        OffHeapBitmapInvertedIndexCreator invCreator = new OffHeapBitmapInvertedIndexCreator(
            _indexDir, fakeFieldSpec, cardinality, _numDocs, _numDocs);
        try {
          int defaultDictId = dictCreator.indexOfSV(getDefaultValue(storedType));
          int ordinal = 0;
          for (int docId = 0; docId < _numDocs; docId++) {
            if (presence.contains(docId)) {
              Object typedValue = values.get(ordinal++);
              invCreator.add(dictCreator.indexOfSV(typedValue));
            } else {
              invCreator.add(defaultDictId);
            }
          }
          invCreator.seal();
        } finally {
          invCreator.close();
        }
      }
    } finally {
      // Close dictCreator after forward index and inverted index are written
      if (dictCreator != null) {
        dictCreator.seal();
        dictCreator.close();
      }
    }

    // 4. Write null value vector
    NullValueVectorCreator nullCreator = new NullValueVectorCreator(_indexDir, materializedCol);
    try {
      for (int docId = 0; docId < _numDocs; docId++) {
        if (!presence.contains(docId)) {
          nullCreator.setNull(docId);
        }
      }
      nullCreator.seal();
    } finally {
      nullCreator.close();
    }

    // 5. Emit metadata
    int maxLength = 0;
    if (storedType == DataType.STRING || storedType == DataType.BYTES) {
      for (Object v : values) {
        int len = storedType == DataType.BYTES ? ((byte[]) v).length
            : ((String) v).getBytes(StandardCharsets.UTF_8).length;
        maxLength = Math.max(maxLength, len);
      }
    }
    emitVirtualColumnMetadata(materializedCol, storedType, cardinality, useDictionary,
        enableInverted && useDictionary, (Object) minValue, (Object) maxValue, maxLength);
  }

  private void writeRawForwardIndex(String materializedCol, DataType storedType,
      RoaringBitmap presence, List<Object> values)
      throws IOException {
    Object defaultVal = getDefaultValue(storedType);
    switch (storedType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE: {
        SingleValueFixedByteRawIndexCreator creator = new SingleValueFixedByteRawIndexCreator(
            _indexDir, ChunkCompressionType.LZ4, materializedCol, _numDocs, storedType);
        try {
          int ordinal = 0;
          for (int docId = 0; docId < _numDocs; docId++) {
            if (presence.contains(docId)) {
              Object val = values.get(ordinal++);
              switch (storedType) {
                case INT:
                  creator.putInt((Integer) val);
                  break;
                case LONG:
                  creator.putLong((Long) val);
                  break;
                case FLOAT:
                  creator.putFloat((Float) val);
                  break;
                case DOUBLE:
                  creator.putDouble((Double) val);
                  break;
                default:
                  break;
              }
            } else {
              switch (storedType) {
                case INT:
                  creator.putInt((Integer) defaultVal);
                  break;
                case LONG:
                  creator.putLong((Long) defaultVal);
                  break;
                case FLOAT:
                  creator.putFloat((Float) defaultVal);
                  break;
                case DOUBLE:
                  creator.putDouble((Double) defaultVal);
                  break;
                default:
                  break;
              }
            }
          }
          creator.seal();
        } finally {
          creator.close();
        }
        break;
      }
      case STRING: {
        int maxLen = 0;
        for (Object v : values) {
          maxLen = Math.max(maxLen, ((String) v).getBytes(StandardCharsets.UTF_8).length);
        }
        maxLen = Math.max(maxLen, 1);
        SingleValueVarByteRawIndexCreator creator = new SingleValueVarByteRawIndexCreator(
            _indexDir, ChunkCompressionType.LZ4, materializedCol, _numDocs, storedType, maxLen);
        try {
          int ordinal = 0;
          for (int docId = 0; docId < _numDocs; docId++) {
            if (presence.contains(docId)) {
              creator.putString((String) values.get(ordinal++));
            } else {
              creator.putString((String) defaultVal);
            }
          }
          creator.seal();
        } finally {
          creator.close();
        }
        break;
      }
      case BYTES: {
        int maxLen = 0;
        for (Object v : values) {
          maxLen = Math.max(maxLen, ((byte[]) v).length);
        }
        maxLen = Math.max(maxLen, 1);
        SingleValueVarByteRawIndexCreator creator = new SingleValueVarByteRawIndexCreator(
            _indexDir, ChunkCompressionType.LZ4, materializedCol, _numDocs, storedType, maxLen);
        try {
          int ordinal = 0;
          for (int docId = 0; docId < _numDocs; docId++) {
            if (presence.contains(docId)) {
              creator.putBytes((byte[]) values.get(ordinal++));
            } else {
              creator.putBytes((byte[]) defaultVal);
            }
          }
          creator.seal();
        } finally {
          creator.close();
        }
        break;
      }
      default:
        throw new IllegalStateException("Unsupported stored type for raw forward index: " + storedType);
    }
  }

  private void writeSparseJsonColumn(List<String> sparseKeys)
      throws IOException {
    String sparseCol = MapNaming.sparseColumnName(_columnName);
    int maxLen = 1;

    // Build per-doc JSON strings
    String[] jsonPerDoc = new String[_numDocs];
    int nonNullCount = 0;
    for (int docId = 0; docId < _numDocs; docId++) {
      Map<String, Object> sparseEntries = new LinkedHashMap<>();
      for (String key : sparseKeys) {
        RoaringBitmap presence = _presenceBitmaps.get(key);
        if (presence != null && presence.contains(docId)) {
          int ordinal = presence.rank(docId) - 1;
          sparseEntries.put(key, _values.get(key).get(ordinal));
        }
      }
      if (!sparseEntries.isEmpty()) {
        try {
          String json = JsonUtils.objectToString(sparseEntries);
          jsonPerDoc[docId] = json;
          maxLen = Math.max(maxLen, json.getBytes(StandardCharsets.UTF_8).length);
          nonNullCount++;
        } catch (IOException e) {
          throw new RuntimeException("Failed to serialize sparse entries for docId " + docId, e);
        }
      }
    }

    // TODO: Add JSON index on the sparse column so that MapFilterOperator can use JSON_MATCH
    //  for sparse key filters instead of falling back to ExpressionFilterOperator (full scan).
    //  Also wire ImmutableMapDataSource.getDataSource() to route sparse keys through
    //  jsonExtractScalar on this column instead of deserializing the full MAP blob per doc.

    // Write STRING forward index for sparse JSON column
    SingleValueVarByteRawIndexCreator fwdCreator = new SingleValueVarByteRawIndexCreator(
        _indexDir, ChunkCompressionType.LZ4, sparseCol, _numDocs, DataType.STRING, maxLen);
    NullValueVectorCreator nullCreator = new NullValueVectorCreator(_indexDir, sparseCol);
    try {
      for (int docId = 0; docId < _numDocs; docId++) {
        if (jsonPerDoc[docId] != null) {
          fwdCreator.putString(jsonPerDoc[docId]);
        } else {
          fwdCreator.putString("");
          nullCreator.setNull(docId);
        }
      }
      fwdCreator.seal();
      nullCreator.seal();
    } finally {
      fwdCreator.close();
      nullCreator.close();
    }

    // Emit metadata for sparse column
    PropertiesConfiguration props = new PropertiesConfiguration();
    String col = sparseCol;
    props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.DATA_TYPE),
        DataType.STRING.name());
    props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.COLUMN_TYPE),
        FieldSpec.FieldType.DIMENSION.name());
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.IS_SINGLE_VALUED), true);
    props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.TOTAL_DOCS),
        _numDocs);
    props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.CARDINALITY),
        nonNullCount);
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.TOTAL_NUMBER_OF_ENTRIES),
        _numDocs);
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.HAS_DICTIONARY), false);
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(col, "hasNullValue"), true);
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.PARENT_MAP_COLUMN),
        _columnName);
    _materializedColumnMetadata.put(sparseCol, props);
  }

  private void emitVirtualColumnMetadata(String materializedCol, DataType storedType, int cardinality,
      boolean hasDictionary, boolean hasInvertedIndex, @Nullable Object minValue, @Nullable Object maxValue,
      int maxLength) {
    PropertiesConfiguration props = new PropertiesConfiguration();
    String col = materializedCol;
    props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.DATA_TYPE),
        storedType.name());
    props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.COLUMN_TYPE),
        FieldSpec.FieldType.DIMENSION.name());
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.IS_SINGLE_VALUED), true);
    props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.CARDINALITY),
        cardinality);
    props.setProperty(V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.TOTAL_DOCS),
        _numDocs);
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.TOTAL_NUMBER_OF_ENTRIES),
        _numDocs);
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.HAS_DICTIONARY), hasDictionary);
    if (hasDictionary) {
      int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(Math.max(cardinality - 1, 0));
      props.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.BITS_PER_ELEMENT),
          numBitsPerValue);
    }
    if (hasInvertedIndex) {
      props.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(col, "hasInvertedIndex"), true);
    }
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(col, "hasNullValue"), true);
    if (maxLength > 0) {
      props.setProperty(
          V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.LENGTH_OF_LONGEST_ELEMENT),
          maxLength);
    }
    BaseSegmentCreator.addColumnMinMaxValueInfo(props, col, minValue, maxValue, storedType);

    // Virtual column markers
    props.setProperty(
        V1Constants.MetadataKeys.Column.getKeyFor(col, V1Constants.MetadataKeys.Column.PARENT_MAP_COLUMN),
        _columnName);
    _materializedColumnMetadata.put(materializedCol, props);
  }

  private boolean shouldUseDictionary(String key, DataType storedType, int numDocsForKey) {
    if (_config.shouldEnableInvertedIndexForKey(key)) {
      return true;
    }
    if (!_config.shouldUseDictionaryForKey(key)) {
      return false;
    }
    Set<String> distinctValues = _distinctValuesPerKey.get(key);
    if (distinctValues == null || distinctValues.isEmpty()) {
      return false;
    }
    int cardinality = distinctValues.size() + 1; // +1 for default value
    int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(Math.max(cardinality - 1, 0));
    long dictIdFwdSize = ((long) _numDocs * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
    long rawSize;
    long dictSize;
    switch (storedType) {
      case INT:
        rawSize = (long) _numDocs * Integer.BYTES;
        dictSize = (long) cardinality * Integer.BYTES;
        break;
      case LONG:
        rawSize = (long) _numDocs * Long.BYTES;
        dictSize = (long) cardinality * Long.BYTES;
        break;
      case FLOAT:
        rawSize = (long) _numDocs * Float.BYTES;
        dictSize = (long) cardinality * Float.BYTES;
        break;
      case DOUBLE:
        rawSize = (long) _numDocs * Double.BYTES;
        dictSize = (long) cardinality * Double.BYTES;
        break;
      case STRING:
      case BYTES: {
        long totalRawBytes = _totalRawBytesPerKey.getOrDefault(key, 0L);
        rawSize = Integer.BYTES + (long) (_numDocs + 1) * Integer.BYTES + totalRawBytes;
        dictSize = 0;
        for (String v : distinctValues) {
          dictSize += Integer.BYTES + v.getBytes(StandardCharsets.UTF_8).length;
        }
        break;
      }
      default:
        return true;
    }
    double ratio = (double) rawSize / (dictSize + dictIdFwdSize);
    return ratio > NO_DICTIONARY_SIZE_RATIO_THRESHOLD;
  }

  private static Object getDefaultValue(DataType storedType) {
    switch (storedType) {
      case INT:
        return 0;
      case LONG:
        return 0L;
      case FLOAT:
        return 0.0f;
      case DOUBLE:
        return 0.0;
      case STRING:
        return "";
      case BYTES:
        return new byte[0];
      default:
        return "";
    }
  }

  private Object buildTypedDistinctArray(String key, DataType storedType) {
    List<Object> values = _values.get(key);
    switch (storedType) {
      case INT: {
        IntOpenHashSet set = new IntOpenHashSet();
        set.add((int) getDefaultValue(storedType));
        for (Object v : values) {
          set.add((int) v);
        }
        int[] sorted = set.toIntArray();
        Arrays.sort(sorted);
        return sorted;
      }
      case LONG: {
        LongOpenHashSet set = new LongOpenHashSet();
        set.add((long) getDefaultValue(storedType));
        for (Object v : values) {
          set.add((long) v);
        }
        long[] sorted = set.toLongArray();
        Arrays.sort(sorted);
        return sorted;
      }
      case FLOAT: {
        FloatOpenHashSet set = new FloatOpenHashSet();
        set.add((float) getDefaultValue(storedType));
        for (Object v : values) {
          set.add((float) v);
        }
        float[] sorted = set.toFloatArray();
        Arrays.sort(sorted);
        return sorted;
      }
      case DOUBLE: {
        DoubleOpenHashSet set = new DoubleOpenHashSet();
        set.add((double) getDefaultValue(storedType));
        for (Object v : values) {
          set.add((double) v);
        }
        double[] sorted = set.toDoubleArray();
        Arrays.sort(sorted);
        return sorted;
      }
      case BYTES: {
        TreeSet<ByteArray> sortedSet = new TreeSet<>();
        sortedSet.add(new ByteArray((byte[]) getDefaultValue(storedType)));
        for (Object v : values) {
          sortedSet.add(new ByteArray((byte[]) v));
        }
        return sortedSet.toArray(new ByteArray[0]);
      }
      case STRING:
      default: {
        Set<String> distinctStrings = _distinctValuesPerKey.getOrDefault(key, Set.of());
        TreeSet<String> sortedSet = new TreeSet<>(distinctStrings);
        String defaultStr = storedType.toString(getDefaultValue(storedType));
        sortedSet.add(defaultStr);
        return sortedSet.toArray(new String[0]);
      }
    }
  }
}
