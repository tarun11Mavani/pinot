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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.creator.SparseMapIndexCreator;
import org.apache.pinot.spi.config.table.SparseMapIndexConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.SparseMapFieldSpec;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * On-heap implementation of {@link SparseMapIndexCreator} for immutable (offline) segments.
 * Accumulates per-document sparse maps in memory and writes a binary index file on seal.
 * Uses more heap memory but avoids disk I/O during indexing.
 */
public class OnHeapSparseMapIndexCreator implements SparseMapIndexCreator {

  private static final Logger LOGGER = LoggerFactory.getLogger(OnHeapSparseMapIndexCreator.class);

  private static final int MAGIC = 0x53504D58;   // "SPMX"
  private static final int VERSION = 2;
  private static final int HEADER_SIZE = 64;
  private static final int KEY_METADATA_ENTRY_SIZE = 69;

  private final File _indexDir;
  private final String _columnName;
  private final Map<String, DataType> _keyTypes;
  private final DataType _defaultValueType;
  private final SparseMapIndexConfig _config;
  private final Set<String> _indexedKeys;
  private final int _maxKeys;

  private final Map<String, RoaringBitmap> _presenceBitmaps = new HashMap<>();
  private final Map<String, List<Object>> _values = new HashMap<>();
  private int _numDocs;
  private int _distinctKeyCount;
  private final Set<String> _droppedKeys = new HashSet<>();

  public OnHeapSparseMapIndexCreator(IndexCreationContext context, SparseMapIndexConfig config)
      throws IOException {
    this(context.getIndexDir(), context.getFieldSpec().getName(),
        (SparseMapFieldSpec) context.getFieldSpec(), config);
  }

  public OnHeapSparseMapIndexCreator(File indexDir, String columnName, SparseMapFieldSpec fieldSpec,
      SparseMapIndexConfig config)
      throws IOException {
    _indexDir = indexDir;
    _columnName = columnName;
    _config = config;
    _indexedKeys = config.getIndexedKeys();
    _maxKeys = config.getMaxKeys();

    Map<String, DataType> keyTypes = fieldSpec.getKeyTypes();
    _keyTypes = keyTypes != null ? new HashMap<>(keyTypes) : new HashMap<>();
    DataType defaultType = fieldSpec.getDefaultValueType();
    _defaultValueType = defaultType != null ? defaultType : DataType.STRING;
  }

  @Override
  public void add(Map<String, Object> sparseMap)
      throws IOException {
    if (sparseMap != null && !sparseMap.isEmpty()) {
      for (Map.Entry<String, Object> entry : sparseMap.entrySet()) {
        String key = entry.getKey();
        if (_indexedKeys != null && !_indexedKeys.contains(key)) {
          continue;
        }
        Object rawValue = entry.getValue();
        if (rawValue == null) {
          // Null values are treated as absent: the key is not recorded for this document.
          // There is no distinction between "key absent" and "key present with null value".
          continue;
        }
        if (!_presenceBitmaps.containsKey(key) && _distinctKeyCount >= _maxKeys) {
          if (_droppedKeys.add(key)) {
            LOGGER.warn(
                "SparseMap index for column '{}' reached maxKeys limit ({}). Dropping key '{}'. "
                    + "Total distinct dropped keys so far: {}.",
                _columnName, _maxKeys, key, _droppedKeys.size());
          }
          continue;
        }
        DataType valueType = _keyTypes.getOrDefault(key, _defaultValueType);
        if (!_presenceBitmaps.containsKey(key)) {
          _presenceBitmaps.put(key, new RoaringBitmap());
          _values.put(key, new ArrayList<>());
          _distinctKeyCount++;
        }
        _presenceBitmaps.get(key).add(_numDocs);
        Object coerced;
        try {
          coerced = coerceValue(rawValue, valueType);
        } catch (ClassCastException | NumberFormatException e) {
          LOGGER.warn(
              "OnHeapSparseMapIndexCreator for column '{}': failed to coerce value '{}' (type {}) to {} for key '{}'."
                  + " Skipping key for this document.",
              _columnName, rawValue, rawValue.getClass().getSimpleName(), valueType, key, e);
          _presenceBitmaps.get(key).remove(_numDocs);
          continue;
        }
        _values.get(key).add(coerced);
      }
    }
    _numDocs++;
  }

  @Override
  public void add(Object value, int dictId)
      throws IOException {
    if (!(value instanceof Map)) {
      return;
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> sparseMap = (Map<String, Object>) value;
    add(sparseMap);
  }

  @Override
  public void add(Object[] values, @Nullable int[] dictIds)
      throws IOException {
    throw new UnsupportedOperationException("SPARSE_MAP is single-value only");
  }

  private Object coerceValue(Object value, DataType dataType) {
    if (value == null) {
      return coerceValueForType(dataType, null);
    }
    DataType storedType = dataType.getStoredType();
    switch (storedType) {
      case INT:
        if (value instanceof Boolean) {
          return (Boolean) value ? 1 : 0;
        }
        if (value instanceof Number) {
          return ((Number) value).intValue();
        }
        return Integer.parseInt(value.toString().trim());
      case LONG:
        if (value instanceof Number) {
          return ((Number) value).longValue();
        }
        return Long.parseLong(value.toString().trim());
      case FLOAT:
        if (value instanceof Number) {
          return ((Number) value).floatValue();
        }
        return Float.parseFloat(value.toString().trim());
      case DOUBLE:
        if (value instanceof Number) {
          return ((Number) value).doubleValue();
        }
        return Double.parseDouble(value.toString().trim());
      case STRING:
        return value.toString();
      case BYTES:
        if (value instanceof byte[]) {
          return value;
        }
        return value.toString().getBytes(StandardCharsets.UTF_8);
      default:
        return value.toString();
    }
  }

  private Object coerceValueForType(DataType dataType, Object nullValue) {
    DataType storedType = dataType.getStoredType();
    switch (storedType) {
      case INT:
        return nullValue != null ? ((Number) nullValue).intValue() : 0;
      case LONG:
        return nullValue != null ? ((Number) nullValue).longValue() : 0L;
      case FLOAT:
        return nullValue != null ? ((Number) nullValue).floatValue() : 0.0f;
      case DOUBLE:
        return nullValue != null ? ((Number) nullValue).doubleValue() : 0.0;
      case STRING:
        return nullValue != null ? nullValue.toString() : "";
      case BYTES:
        return nullValue instanceof byte[] ? nullValue : new byte[0];
      default:
        return "";
    }
  }

  @Override
  public void seal()
      throws IOException {
    List<String> sortedKeys = new ArrayList<>(_presenceBitmaps.keySet());
    Collections.sort(sortedKeys);
    int numKeys = sortedKeys.size();

    byte[] keyDictionarySection = buildKeyDictionarySection(sortedKeys);
    byte[] keyMetadataSection = new byte[numKeys * KEY_METADATA_ENTRY_SIZE];
    Map<String, TreeMap<String, RoaringBitmap>> cachedValueToDocIds = new HashMap<>();
    byte[] perKeyDataSection = buildPerKeyDataSection(sortedKeys, keyMetadataSection, cachedValueToDocIds);
    byte[] valueDictionarySection = buildValueDictionarySection(sortedKeys, cachedValueToDocIds);

    long keyDictionaryOffset = HEADER_SIZE;
    long keyMetadataOffset = keyDictionaryOffset + keyDictionarySection.length;
    long perKeyDataOffset = keyMetadataOffset + keyMetadataSection.length;
    long valueDictionaryOffset = perKeyDataOffset + perKeyDataSection.length;

    File indexFile = new File(_indexDir, _columnName + V1Constants.Indexes.SPARSE_MAP_INDEX_FILE_EXTENSION);
    try (FileOutputStream fos = new FileOutputStream(indexFile);
        DataOutputStream dos = new DataOutputStream(fos)) {
      writeHeader(dos, numKeys, keyDictionaryOffset, keyMetadataOffset, perKeyDataOffset, valueDictionaryOffset);
      dos.write(keyDictionarySection);
      dos.write(keyMetadataSection);
      dos.write(perKeyDataSection);
      dos.write(valueDictionarySection);
    }
  }

  private byte[] buildKeyDictionarySection(List<String> sortedKeys)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(baos)) {
      dos.writeInt(sortedKeys.size());
      for (String key : sortedKeys) {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(keyBytes.length);
        dos.write(keyBytes);
      }
    }
    return baos.toByteArray();
  }

  private byte[] buildPerKeyDataSection(List<String> sortedKeys, byte[] keyMetadataSection,
      Map<String, TreeMap<String, RoaringBitmap>> cachedValueToDocIds)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(baos)) {
      long currentOffset = 0;

      for (int i = 0; i < sortedKeys.size(); i++) {
        String key = sortedKeys.get(i);
        RoaringBitmap presence = _presenceBitmaps.get(key);
        List<Object> values = _values.get(key);
        DataType dataType = _keyTypes.getOrDefault(key, _defaultValueType);
        DataType storedType = dataType.getStoredType();

        byte[] presenceBytes = RoaringBitmapUtils.serialize(presence);
        long presenceOffset = currentOffset;
        dos.write(presenceBytes);
        currentOffset += presenceBytes.length;

        byte[] forwardBytes = buildForwardIndex(values, storedType);
        long forwardOffset = currentOffset;
        dos.write(forwardBytes);
        currentOffset += forwardBytes.length;

        boolean enableInverted = _config.shouldEnableInvertedIndexForKey(key);
        // Build inverted index and collect value→docId mappings for dictId forward index
        TreeMap<String, RoaringBitmap> valueToDocIds = null;
        byte[] invertedBytes;
        if (enableInverted) {
          valueToDocIds = buildValueToDocIds(presence, values, storedType);
          cachedValueToDocIds.put(key, valueToDocIds);
          invertedBytes = serializeInvertedIndex(valueToDocIds);
        } else {
          invertedBytes = new byte[0];
        }
        long invertedOffset = enableInverted ? currentOffset : 0;
        if (enableInverted) {
          dos.write(invertedBytes);
          currentOffset += invertedBytes.length;
        }

        // Build dictId forward index for keys with inverted index
        long dictIdFwdOffset = 0;
        long dictIdFwdLength = 0;
        if (enableInverted && valueToDocIds != null) {
          byte[] dictIdFwdBytes = buildDictIdForwardIndex(valueToDocIds, storedType, presence);
          dictIdFwdOffset = currentOffset;
          dictIdFwdLength = dictIdFwdBytes.length;
          dos.write(dictIdFwdBytes);
          currentOffset += dictIdFwdBytes.length;
        }

        int entryOffset = i * KEY_METADATA_ENTRY_SIZE;
        keyMetadataSection[entryOffset] = (byte) storedType.ordinal();
        writeInt(keyMetadataSection, entryOffset + 1, values.size());
        writeLong(keyMetadataSection, entryOffset + 5, presenceOffset);
        writeLong(keyMetadataSection, entryOffset + 13, presenceBytes.length);
        writeLong(keyMetadataSection, entryOffset + 21, forwardOffset);
        writeLong(keyMetadataSection, entryOffset + 29, forwardBytes.length);
        writeLong(keyMetadataSection, entryOffset + 37, invertedOffset);
        writeLong(keyMetadataSection, entryOffset + 45, invertedBytes.length);
        writeLong(keyMetadataSection, entryOffset + 53, dictIdFwdOffset);
        writeLong(keyMetadataSection, entryOffset + 61, dictIdFwdLength);
      }
    }
    return baos.toByteArray();
  }

  private static void writeInt(byte[] buf, int offset, int value) {
    buf[offset] = (byte) (value >> 24);
    buf[offset + 1] = (byte) (value >> 16);
    buf[offset + 2] = (byte) (value >> 8);
    buf[offset + 3] = (byte) (value);
  }

  private static void writeLong(byte[] buf, int offset, long value) {
    for (int i = 0; i < 8; i++) {
      buf[offset + i] = (byte) (value >> ((7 - i) * 8));
    }
  }

  private byte[] buildForwardIndex(List<Object> values, DataType storedType)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    writeForwardIndex(dos, values, storedType);
    return baos.toByteArray();
  }

  private void writeForwardIndex(DataOutputStream dos, List<Object> values, DataType storedType)
      throws IOException {
    switch (storedType) {
      case INT:
        for (Object v : values) {
          dos.writeInt((Integer) v);
        }
        break;
      case LONG:
        for (Object v : values) {
          dos.writeLong((Long) v);
        }
        break;
      case FLOAT:
        for (Object v : values) {
          dos.writeFloat((Float) v);
        }
        break;
      case DOUBLE:
        for (Object v : values) {
          dos.writeDouble((Double) v);
        }
        break;
      case STRING: {
        int numValues = values.size();
        dos.writeInt(numValues);
        ByteArrayOutputStream dataBaos = new ByteArrayOutputStream();
        int[] offsets = new int[numValues + 1];
        int offset = 0;
        for (int i = 0; i < numValues; i++) {
          offsets[i] = offset;
          byte[] b = ((String) values.get(i)).getBytes(StandardCharsets.UTF_8);
          dataBaos.write(b);
          offset += b.length;
        }
        offsets[numValues] = offset;
        for (int o : offsets) {
          dos.writeInt(o);
        }
        dos.write(dataBaos.toByteArray());
        break;
      }
      case BYTES: {
        int numValues = values.size();
        dos.writeInt(numValues);
        ByteArrayOutputStream dataBaos = new ByteArrayOutputStream();
        int[] offsets = new int[numValues + 1];
        int offset = 0;
        for (int i = 0; i < numValues; i++) {
          offsets[i] = offset;
          byte[] b = (byte[]) values.get(i);
          dataBaos.write(b);
          offset += b.length;
        }
        offsets[numValues] = offset;
        for (int o : offsets) {
          dos.writeInt(o);
        }
        dos.write(dataBaos.toByteArray());
        break;
      }
      default:
        throw new IllegalStateException("Unsupported stored type for forward index: " + storedType);
    }
  }

  private TreeMap<String, RoaringBitmap> buildValueToDocIds(RoaringBitmap presence, List<Object> values,
      DataType storedType) {
    TreeMap<String, RoaringBitmap> valueToDocIds = new TreeMap<>();
    int ordinal = 0;
    for (int docId : presence) {
      Object value = values.get(ordinal);
      String keyRep = storedType.toString(value);
      valueToDocIds.computeIfAbsent(keyRep, k -> new RoaringBitmap()).add(docId);
      ordinal++;
    }
    return valueToDocIds;
  }

  private byte[] serializeInvertedIndex(TreeMap<String, RoaringBitmap> valueToDocIds)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeInt(valueToDocIds.size());
    for (Map.Entry<String, RoaringBitmap> entry : valueToDocIds.entrySet()) {
      byte[] valueBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
      dos.writeInt(valueBytes.length);
      dos.write(valueBytes);
      byte[] bitmapBytes = RoaringBitmapUtils.serialize(entry.getValue());
      dos.writeInt(bitmapBytes.length);
      dos.write(bitmapBytes);
    }
    return baos.toByteArray();
  }

  /// Builds a bit-packed dictId forward index for all numDocs documents.
  /// Documents without the key get the default value's dictId.
  private byte[] buildDictIdForwardIndex(TreeMap<String, RoaringBitmap> valueToDocIds,
      DataType storedType, RoaringBitmap presence)
      throws IOException {
    // Build sorted distinct values array, merging default value
    String defaultValue = SparseMapKeyDictionary.getDefaultValueString(storedType);
    String[] distinctValues = valueToDocIds.keySet().toArray(new String[0]);

    // Merge default value into sorted array
    Set<String> allSet = new HashSet<>(java.util.Arrays.asList(distinctValues));
    allSet.add(defaultValue);
    String[] allValues = allSet.toArray(new String[0]);

    // Sort numerically for numeric types, lexicographically for strings
    sortValues(allValues, storedType);

    // Build value → dictId mapping
    Map<String, Integer> valueToDictId = new HashMap<>();
    for (int i = 0; i < allValues.length; i++) {
      valueToDictId.put(allValues[i], i);
    }

    int defaultDictId = valueToDictId.get(defaultValue);
    int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(allValues.length - 1);

    // Build dictId array for all docs
    int[] dictIdArray = new int[_numDocs];
    java.util.Arrays.fill(dictIdArray, defaultDictId);
    for (Map.Entry<String, RoaringBitmap> entry : valueToDocIds.entrySet()) {
      int dictId = valueToDictId.get(entry.getKey());
      for (int docId : entry.getValue()) {
        dictIdArray[docId] = dictId;
      }
    }

    // Write bit-packed
    long bufferSize = ((long) _numDocs * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
    byte[] buffer = new byte[(int) bufferSize];
    writeBitPacked(buffer, dictIdArray, _numDocs, numBitsPerValue);
    return buffer;
  }

  /// Writes bit-packed integers into a byte array (big-endian bit order).
  private static void writeBitPacked(byte[] buffer, int[] values, int numValues, int numBitsPerValue) {
    long bitOffset = 0;
    for (int i = 0; i < numValues; i++) {
      int value = values[i];
      for (int bit = numBitsPerValue - 1; bit >= 0; bit--) {
        if (((value >> bit) & 1) == 1) {
          int byteIndex = (int) (bitOffset / 8);
          int bitIndex = (int) (7 - (bitOffset % 8));
          buffer[byteIndex] |= (1 << bitIndex);
        }
        bitOffset++;
      }
    }
  }

  /**
   * Sorts string-encoded values using numeric comparison for numeric types,
   * or lexicographic comparison for STRING/BYTES.
   */
  private static void sortValues(String[] values, DataType storedType) {
    Comparator<String> cmp;
    switch (storedType) {
      case INT:
        cmp = Comparator.comparingInt(Integer::parseInt);
        break;
      case LONG:
        cmp = Comparator.comparingLong(Long::parseLong);
        break;
      case FLOAT:
        cmp = (a, b) -> Float.compare(Float.parseFloat(a), Float.parseFloat(b));
        break;
      case DOUBLE:
        cmp = Comparator.comparingDouble(Double::parseDouble);
        break;
      default:
        cmp = null;
        break;
    }
    if (cmp != null) {
      java.util.Arrays.sort(values, cmp);
    } else {
      java.util.Arrays.sort(values);
    }
  }

  /// Builds the value dictionary section written after all per-key data.
  /// For each key with inverted index: numDistinctValues(int), numBitsPerValue(int),
  /// then [valueLen(int) + valueBytes] × numDistinctValues.
  private byte[] buildValueDictionarySection(List<String> sortedKeys,
      Map<String, TreeMap<String, RoaringBitmap>> cachedValueToDocIds)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    for (String key : sortedKeys) {
      boolean enableInverted = _config.shouldEnableInvertedIndexForKey(key);
      if (!enableInverted) {
        continue;
      }

      DataType dataType = _keyTypes.getOrDefault(key, _defaultValueType);
      DataType storedType = dataType.getStoredType();

      TreeMap<String, RoaringBitmap> valueToDocIds = cachedValueToDocIds.get(key);

      // Merge default value
      String defaultValue = SparseMapKeyDictionary.getDefaultValueString(storedType);
      String[] distinctValues = valueToDocIds != null ? valueToDocIds.keySet().toArray(new String[0]) : new String[0];

      Set<String> allSet = new HashSet<>(java.util.Arrays.asList(distinctValues));
      allSet.add(defaultValue);
      String[] allValues = allSet.toArray(new String[0]);

      // Sort numerically for numeric types, lexicographically for strings
      sortValues(allValues, storedType);

      int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(allValues.length - 1);
      dos.writeInt(allValues.length);
      dos.writeInt(numBitsPerValue);
      for (String v : allValues) {
        byte[] vBytes = v.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(vBytes.length);
        dos.write(vBytes);
      }
    }
    return baos.toByteArray();
  }

  private void writeHeader(DataOutputStream dos, int numKeys, long keyDictOffset, long keyMetaOffset,
      long perKeyOffset, long valueDictOffset)
      throws IOException {
    dos.writeInt(MAGIC);
    dos.writeInt(VERSION);
    dos.writeInt(numKeys);
    dos.writeInt(_numDocs);
    dos.writeLong(keyDictOffset);
    dos.writeLong(keyMetaOffset);
    dos.writeLong(perKeyOffset);
    dos.writeLong(valueDictOffset);
    // 64 - (4 ints * 4 bytes) - (4 longs * 8 bytes) = 64 - 16 - 32 = 16 bytes padding
    for (int i = 0; i < 16; i++) {
      dos.write(0);
    }
  }

  @Override
  public void close()
      throws IOException {
  }
}
