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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.segment.local.io.util.PinotDataBitSet;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.SparseMapIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Memory-mapped immutable reader for the SparseMap index.
 * Provides O(1) typed key lookup via presence bitmap rank operations.
 *
 * <p>Binary format (written by {@link OnHeapSparseMapIndexCreator}):
 * <ul>
 *   <li>Header (64 bytes): version(int), numKeys(int), numDocs(int), keyDictOffset(long),
 *       keyMetaOffset(long), perKeyDataOffset(long), padding</li>
 *   <li>Key dictionary: numKeys(int), per key: keyLen(int) + keyBytes</li>
 *   <li>Key metadata (53 bytes/key, big-endian): storedTypeOrdinal(byte), numDocs(int),
 *       presenceOffset(long), presenceLen(long), fwdOffset(long), fwdLen(long),
 *       invOffset(long), invLen(long)</li>
 *   <li>Per-key data: presence bitmap, typed forward index, optional inverted index</li>
 * </ul>
 * <p>Null-means-absent policy: keys with null values are not recorded during ingestion.
 * A key absent from the presence bitmap is indistinguishable from a key explicitly set to null.
 */
public class ImmutableSparseMapIndexReader implements SparseMapIndexReader {

  private static final int MAGIC = 0x53504D58;   // "SPMX"
  private static final int CURRENT_VERSION = 2;
  private static final int HEADER_SIZE = 64;
  private static final int KEY_METADATA_ENTRY_SIZE = 69;

  private final PinotDataBuffer _dataBuffer;
  private final int _numDocs;
  private final int _numKeys;

  // per-key data (indexed by keyId)
  private final String[] _keys;
  private final Map<String, Integer> _keyToId;
  private final DataType[] _keyStoredTypes;
  private final int[] _numDocsPerKey;
  private final ImmutableRoaringBitmap[] _presenceBitmaps;

  // offsets/lengths within _dataBuffer for forward and inverted indexes
  private final long _perKeyDataSectionOffset;
  private final long[] _fwdOffsets;
  private final long[] _fwdLengths;
  private final long[] _invOffsets;
  private final long[] _invLengths;
  private final long[] _dictIdFwdOffsets;
  private final long[] _dictIdFwdLengths;

  // value dictionary section
  private final long _valueDictionarySectionOffset;
  private final Map<String, SparseMapKeyDictionary> _keyDictionaries;

  public ImmutableSparseMapIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
      throws IOException {
    _dataBuffer = dataBuffer;

    // ---- Parse Header (BIG_ENDIAN throughout) ----
    byte[] headerBytes = new byte[HEADER_SIZE];
    dataBuffer.copyTo(0, headerBytes, 0, HEADER_SIZE);
    ByteBuffer headerBuf = ByteBuffer.wrap(headerBytes).order(ByteOrder.BIG_ENDIAN);
    int magic = headerBuf.getInt();
    if (magic != MAGIC) {
      throw new IOException(
          String.format("Invalid SparseMap index: expected magic 0x%08X but got 0x%08X", MAGIC, magic));
    }
    int version = headerBuf.getInt();
    if (version > CURRENT_VERSION) {
      throw new IOException(
          "Unsupported SparseMap index version: " + version + " (max supported: " + CURRENT_VERSION + ")");
    }
    _numKeys = headerBuf.getInt();
    _numDocs = headerBuf.getInt();
    long keyDictOffset = headerBuf.getLong();
    long keyMetaOffset = headerBuf.getLong();
    _perKeyDataSectionOffset = headerBuf.getLong();
    _valueDictionarySectionOffset = headerBuf.getLong();

    // ---- Parse Key Dictionary (big-endian) ----
    _keys = new String[_numKeys];
    _keyToId = new HashMap<>(_numKeys * 2);
    int dictPos = (int) keyDictOffset;
    byte[] dictCountBytes = new byte[4];
    dataBuffer.copyTo(dictPos, dictCountBytes, 0, 4);
    /* int dictNumKeys = */ ByteBuffer.wrap(dictCountBytes).order(ByteOrder.BIG_ENDIAN).getInt();
    dictPos += 4;
    for (int i = 0; i < _numKeys; i++) {
      byte[] lenBytes = new byte[4];
      dataBuffer.copyTo(dictPos, lenBytes, 0, 4);
      int keyLen = ByteBuffer.wrap(lenBytes).order(ByteOrder.BIG_ENDIAN).getInt();
      dictPos += 4;
      byte[] keyBytes = new byte[keyLen];
      dataBuffer.copyTo(dictPos, keyBytes, 0, keyLen);
      _keys[i] = new String(keyBytes, StandardCharsets.UTF_8);
      _keyToId.put(_keys[i], i);
      dictPos += keyLen;
    }

    // ---- Parse Key Metadata (BIG_ENDIAN, 69 bytes per key) ----
    _keyStoredTypes = new DataType[_numKeys];
    _numDocsPerKey = new int[_numKeys];
    _presenceBitmaps = new ImmutableRoaringBitmap[_numKeys];
    _fwdOffsets = new long[_numKeys];
    _fwdLengths = new long[_numKeys];
    _invOffsets = new long[_numKeys];
    _invLengths = new long[_numKeys];
    _dictIdFwdOffsets = new long[_numKeys];
    _dictIdFwdLengths = new long[_numKeys];

    byte[] metaBlock = new byte[_numKeys * KEY_METADATA_ENTRY_SIZE];
    dataBuffer.copyTo(keyMetaOffset, metaBlock, 0, metaBlock.length);
    ByteBuffer metaBuf = ByteBuffer.wrap(metaBlock).order(ByteOrder.BIG_ENDIAN);
    DataType[] allTypes = DataType.values();

    for (int i = 0; i < _numKeys; i++) {
      int storedTypeOrdinal = metaBuf.get() & 0xFF;
      if (storedTypeOrdinal >= allTypes.length) {
        throw new IOException(
            "Invalid SparseMap index: unknown DataType ordinal " + storedTypeOrdinal
                + " for key index " + i + " (max=" + (allTypes.length - 1) + ")");
      }
      _keyStoredTypes[i] = allTypes[storedTypeOrdinal];
      _numDocsPerKey[i] = metaBuf.getInt();
      long presenceOffset = metaBuf.getLong();
      long presenceLen = metaBuf.getLong();
      _fwdOffsets[i] = metaBuf.getLong();
      _fwdLengths[i] = metaBuf.getLong();
      _invOffsets[i] = metaBuf.getLong();
      _invLengths[i] = metaBuf.getLong();
      _dictIdFwdOffsets[i] = metaBuf.getLong();
      _dictIdFwdLengths[i] = metaBuf.getLong();

      // Load presence bitmap
      byte[] bitmapBytes = new byte[(int) presenceLen];
      dataBuffer.copyTo(_perKeyDataSectionOffset + presenceOffset, bitmapBytes, 0, bitmapBytes.length);
      _presenceBitmaps[i] = new ImmutableRoaringBitmap(ByteBuffer.wrap(bitmapBytes));
    }

    // ---- Parse Value Dictionary Section ----
    _keyDictionaries = new HashMap<>();
    if (_valueDictionarySectionOffset > 0) {
      long pos = _valueDictionarySectionOffset;
      for (int i = 0; i < _numKeys; i++) {
        if (_dictIdFwdLengths[i] == 0) {
          continue;
        }
        byte[] countBytes = new byte[4];
        dataBuffer.copyTo(pos, countBytes, 0, 4);
        int numDistinctValues = ByteBuffer.wrap(countBytes).order(ByteOrder.BIG_ENDIAN).getInt();
        pos += 4;

        byte[] bitsBytes = new byte[4];
        dataBuffer.copyTo(pos, bitsBytes, 0, 4);
        /* int numBitsPerValue = */ ByteBuffer.wrap(bitsBytes).order(ByteOrder.BIG_ENDIAN).getInt();
        pos += 4;

        String[] values = new String[numDistinctValues];
        for (int v = 0; v < numDistinctValues; v++) {
          byte[] vLenBytes = new byte[4];
          dataBuffer.copyTo(pos, vLenBytes, 0, 4);
          int vLen = ByteBuffer.wrap(vLenBytes).order(ByteOrder.BIG_ENDIAN).getInt();
          pos += 4;
          byte[] vBytes = new byte[vLen];
          dataBuffer.copyTo(pos, vBytes, 0, vLen);
          values[v] = new String(vBytes, StandardCharsets.UTF_8);
          pos += vLen;
        }
        _keyDictionaries.put(_keys[i], new SparseMapKeyDictionary(_keyStoredTypes[i], values));
      }
    }
  }

  @Override
  public Set<String> getKeys() {
    Set<String> keys = new HashSet<>(_numKeys * 2);
    for (String key : _keys) {
      keys.add(key);
    }
    return keys;
  }

  @Nullable
  @Override
  public DataType getKeyValueType(String key) {
    Integer keyId = _keyToId.get(key);
    return keyId != null ? _keyStoredTypes[keyId] : null;
  }

  @Override
  public int getNumDocsWithKey(String key) {
    Integer keyId = _keyToId.get(key);
    return keyId != null ? _numDocsPerKey[keyId] : 0;
  }

  @Override
  public ImmutableRoaringBitmap getPresenceBitmap(String key) {
    Integer keyId = _keyToId.get(key);
    return keyId != null ? _presenceBitmaps[keyId] : ImmutableRoaringBitmap.bitmapOf();
  }

  @Override
  public int getInt(int docId, String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null) {
      return 0;
    }
    ImmutableRoaringBitmap bitmap = _presenceBitmaps[keyId];
    if (!bitmap.contains(docId)) {
      return 0;
    }
    int ordinal = bitmap.rank(docId) - 1;
    long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) ordinal * Integer.BYTES;
    byte[] bytes = new byte[Integer.BYTES];
    _dataBuffer.copyTo(bufOffset, bytes, 0, Integer.BYTES);
    return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt();
  }

  @Override
  public long getLong(int docId, String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null) {
      return 0L;
    }
    ImmutableRoaringBitmap bitmap = _presenceBitmaps[keyId];
    if (!bitmap.contains(docId)) {
      return 0L;
    }
    int ordinal = bitmap.rank(docId) - 1;
    long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) ordinal * Long.BYTES;
    byte[] bytes = new byte[Long.BYTES];
    _dataBuffer.copyTo(bufOffset, bytes, 0, Long.BYTES);
    return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getLong();
  }

  @Override
  public float getFloat(int docId, String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null) {
      return 0.0f;
    }
    ImmutableRoaringBitmap bitmap = _presenceBitmaps[keyId];
    if (!bitmap.contains(docId)) {
      return 0.0f;
    }
    int ordinal = bitmap.rank(docId) - 1;
    long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) ordinal * Float.BYTES;
    byte[] bytes = new byte[Float.BYTES];
    _dataBuffer.copyTo(bufOffset, bytes, 0, Float.BYTES);
    return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getFloat();
  }

  @Override
  public double getDouble(int docId, String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null) {
      return 0.0;
    }
    ImmutableRoaringBitmap bitmap = _presenceBitmaps[keyId];
    if (!bitmap.contains(docId)) {
      return 0.0;
    }
    int ordinal = bitmap.rank(docId) - 1;
    long bufOffset = _perKeyDataSectionOffset + _fwdOffsets[keyId] + (long) ordinal * Double.BYTES;
    byte[] bytes = new byte[Double.BYTES];
    _dataBuffer.copyTo(bufOffset, bytes, 0, Double.BYTES);
    return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getDouble();
  }

  @Override
  public String getString(int docId, String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null) {
      return "";
    }
    ImmutableRoaringBitmap bitmap = _presenceBitmaps[keyId];
    if (!bitmap.contains(docId)) {
      return "";
    }
    int ordinal = bitmap.rank(docId) - 1;
    return readStringAtOrdinal(keyId, ordinal);
  }

  @Override
  public byte[] getBytes(int docId, String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null) {
      return new byte[0];
    }
    ImmutableRoaringBitmap bitmap = _presenceBitmaps[keyId];
    if (!bitmap.contains(docId)) {
      return new byte[0];
    }
    int ordinal = bitmap.rank(docId) - 1;
    return readBytesAtOrdinal(keyId, ordinal);
  }

  private String readStringAtOrdinal(int keyId, int ordinal) {
    byte[] raw = readBytesAtOrdinal(keyId, ordinal);
    return new String(raw, StandardCharsets.UTF_8);
  }

  /**
   * Reads variable-length bytes from the STRING/BYTES forward index at the given ordinal.
   * Format: numValues(int), offsets(int[numValues+1]), data(bytes)
   */
  private byte[] readBytesAtOrdinal(int keyId, int ordinal) {
    // Header: numValues(4 bytes), then offsets array ((numValues+1)*4 bytes), then data
    long fwdBase = _perKeyDataSectionOffset + _fwdOffsets[keyId];
    byte[] numValBytes = new byte[4];
    _dataBuffer.copyTo(fwdBase, numValBytes, 0, 4);
    int numValues = ByteBuffer.wrap(numValBytes).order(ByteOrder.BIG_ENDIAN).getInt();
    if (numValues != _numDocsPerKey[keyId]) {
      throw new IllegalStateException(
          "SparseMap forward index corrupt for key '" + _keys[keyId]
              + "': numValues=" + numValues + " but presence cardinality=" + _numDocsPerKey[keyId]);
    }

    // Read the two offsets for this ordinal: offsets[ordinal] and offsets[ordinal+1]
    long offsetBase = fwdBase + 4 + (long) ordinal * Integer.BYTES;
    byte[] offsetBytes = new byte[8];
    _dataBuffer.copyTo(offsetBase, offsetBytes, 0, 8);
    ByteBuffer ob = ByteBuffer.wrap(offsetBytes).order(ByteOrder.BIG_ENDIAN);
    int startOffset = ob.getInt();
    int endOffset = ob.getInt();

    int dataLen = endOffset - startOffset;
    if (dataLen <= 0) {
      return new byte[0];
    }

    // Data starts after the offsets array: fwdBase + 4 (numValues) + (numValues+1)*4 (offsets)
    long dataBase = fwdBase + 4 + (long) (numValues + 1) * Integer.BYTES;
    byte[] result = new byte[dataLen];
    _dataBuffer.copyTo(dataBase + startOffset, result, 0, dataLen);
    return result;
  }

  @Nullable
  @Override
  public ImmutableRoaringBitmap getDocsWithKeyValue(String key, Object value) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null || _invLengths[keyId] == 0) {
      return null;
    }
    String valueStr = _keyStoredTypes[keyId].toString(value);
    byte[] valueBytes = valueStr.getBytes(StandardCharsets.UTF_8);

    long invBase = _perKeyDataSectionOffset + _invOffsets[keyId];
    byte[] numUniqueBytes = new byte[4];
    _dataBuffer.copyTo(invBase, numUniqueBytes, 0, 4);
    int numUnique = ByteBuffer.wrap(numUniqueBytes).order(ByteOrder.BIG_ENDIAN).getInt();

    long pos = invBase + 4;
    for (int i = 0; i < numUnique; i++) {
      byte[] vLenBytes = new byte[4];
      _dataBuffer.copyTo(pos, vLenBytes, 0, 4);
      int vLen = ByteBuffer.wrap(vLenBytes).order(ByteOrder.BIG_ENDIAN).getInt();
      pos += 4;
      byte[] vBytes = new byte[vLen];
      _dataBuffer.copyTo(pos, vBytes, 0, vLen);
      pos += vLen;

      byte[] bLenBytes = new byte[4];
      _dataBuffer.copyTo(pos, bLenBytes, 0, 4);
      int bLen = ByteBuffer.wrap(bLenBytes).order(ByteOrder.BIG_ENDIAN).getInt();
      pos += 4;

      if (vLen == valueBytes.length && java.util.Arrays.equals(vBytes, valueBytes)) {
        byte[] bitmapBytes = new byte[bLen];
        _dataBuffer.copyTo(pos, bitmapBytes, 0, bLen);
        return new ImmutableRoaringBitmap(ByteBuffer.wrap(bitmapBytes));
      }
      pos += bLen;
    }
    return null;
  }

  @Override
  public boolean hasInvertedIndex(String key) {
    Integer keyId = _keyToId.get(key);
    return keyId != null && _invLengths[keyId] > 0;
  }

  @Override
  @Nullable
  public String[] getDistinctValuesForKey(String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null || _invLengths[keyId] == 0) {
      return null;
    }

    long invBase = _perKeyDataSectionOffset + _invOffsets[keyId];
    byte[] numUniqueBytes = new byte[4];
    _dataBuffer.copyTo(invBase, numUniqueBytes, 0, 4);
    int numUnique = ByteBuffer.wrap(numUniqueBytes).order(ByteOrder.BIG_ENDIAN).getInt();

    String[] distinctValues = new String[numUnique];
    long pos = invBase + 4;
    for (int i = 0; i < numUnique; i++) {
      byte[] vLenBytes = new byte[4];
      _dataBuffer.copyTo(pos, vLenBytes, 0, 4);
      int vLen = ByteBuffer.wrap(vLenBytes).order(ByteOrder.BIG_ENDIAN).getInt();
      pos += 4;

      byte[] vBytes = new byte[vLen];
      _dataBuffer.copyTo(pos, vBytes, 0, vLen);
      distinctValues[i] = new String(vBytes, StandardCharsets.UTF_8);
      pos += vLen;

      // Skip bitmap: read bLen and advance past bitmap bytes
      byte[] bLenBytes = new byte[4];
      _dataBuffer.copyTo(pos, bLenBytes, 0, 4);
      int bLen = ByteBuffer.wrap(bLenBytes).order(ByteOrder.BIG_ENDIAN).getInt();
      pos += 4 + bLen;
    }
    return distinctValues;
  }

  @Override
  public DataSource getKeyDataSource(String key) {
    // Implemented in SparseMapDataSource (Task 15)
    return null;
  }

  /// Returns a FixedBitIntReaderWriter for reading bit-packed dictIds for the given key,
  /// or null if no dictId forward index is available.
  @Nullable
  public FixedBitIntReaderWriter getDictIdReader(String key) {
    Integer keyId = _keyToId.get(key);
    if (keyId == null || _dictIdFwdLengths[keyId] == 0) {
      return null;
    }
    SparseMapKeyDictionary dict = _keyDictionaries.get(key);
    if (dict == null) {
      return null;
    }
    int numBitsPerValue = PinotDataBitSet.getNumBitsPerValue(dict.length() - 1);
    long offset = _perKeyDataSectionOffset + _dictIdFwdOffsets[keyId];
    PinotDataBuffer slice = _dataBuffer.view(offset, offset + _dictIdFwdLengths[keyId]);
    return new FixedBitIntReaderWriter(slice, _numDocs, numBitsPerValue);
  }

  /// Returns the cached SparseMapKeyDictionary for the given key, or null if not available.
  @Nullable
  public SparseMapKeyDictionary getKeyDictionary(String key) {
    return _keyDictionaries.get(key);
  }

  /// Returns the total number of documents in this index.
  public int getNumDocs() {
    return _numDocs;
  }

  @Override
  public Map<String, Object> getMap(int docId) {
    Map<String, Object> result = new HashMap<>();
    for (int i = 0; i < _numKeys; i++) {
      if (!_presenceBitmaps[i].contains(docId)) {
        continue;
      }
      String key = _keys[i];
      Object value;
      switch (_keyStoredTypes[i]) {
        case INT:
          value = getInt(docId, key);
          break;
        case LONG:
          value = getLong(docId, key);
          break;
        case FLOAT:
          value = getFloat(docId, key);
          break;
        case DOUBLE:
          value = getDouble(docId, key);
          break;
        case BYTES:
          value = getBytes(docId, key);
          break;
        default:
          value = getString(docId, key);
          break;
      }
      result.put(key, value);
    }
    return result;
  }

  @Override
  public void close()
      throws IOException {
    // PinotDataBuffer is closed by the segment
  }
}
