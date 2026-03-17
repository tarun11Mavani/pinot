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
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.SparseMapIndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * A per-key {@link ForwardIndexReader} backed by a {@link SparseMapIndexReader}.
 *
 * <p>Each instance is bound to a single key within a SPARSE_MAP column. Reads for document IDs
 * that do not contain the key return the type-appropriate zero/empty default value; the caller can
 * combine this with the presence bitmap ({@link SparseMapIndexReader#getPresenceBitmap}) when null
 * semantics are required.
 *
 * <p>When a {@link SparseMapKeyDictionary} is provided, this reader supports dictionary-encoded
 * access via {@link #readDictIds}, enabling dictionary-based GROUP BY operations.
 *
 * <p>No context is needed because the {@link SparseMapIndexReader} implementations maintain their
 * own internal state; {@link #createContext()} therefore returns {@code null}.
 *
 * <p>Lifecycle: this reader does NOT own the underlying {@link SparseMapIndexReader}—closing this
 * reader is a no-op. The owning {@link SparseMapDataSource} is responsible for closing the reader.
 */
public class SparseMapKeyForwardIndexReader implements ForwardIndexReader<ForwardIndexReaderContext> {

  private final SparseMapIndexReader _sparseMapIndexReader;
  private final String _key;
  private final DataType _storedType;
  @Nullable
  private final SparseMapKeyDictionary _dictionary;
  @Nullable
  private final FixedBitIntReaderWriter _dictIdReader;
  private final int _defaultDictId;

  public SparseMapKeyForwardIndexReader(SparseMapIndexReader sparseMapIndexReader, String key,
      DataType storedType) {
    this(sparseMapIndexReader, key, storedType, null, null);
  }

  public SparseMapKeyForwardIndexReader(SparseMapIndexReader sparseMapIndexReader, String key,
      DataType storedType, @Nullable SparseMapKeyDictionary dictionary) {
    this(sparseMapIndexReader, key, storedType, dictionary, null);
  }

  public SparseMapKeyForwardIndexReader(SparseMapIndexReader sparseMapIndexReader, String key,
      DataType storedType, @Nullable SparseMapKeyDictionary dictionary,
      @Nullable FixedBitIntReaderWriter dictIdReader) {
    _sparseMapIndexReader = sparseMapIndexReader;
    _key = key;
    _storedType = storedType;
    _dictionary = dictionary;
    _dictIdReader = dictIdReader;
    if (dictionary != null) {
      String defaultValueStr = SparseMapKeyDictionary.getDefaultValueString(storedType);
      int idx = dictionary.indexOf(defaultValueStr);
      _defaultDictId = idx >= 0 ? idx : 0;
    } else {
      _defaultDictId = 0;
    }
  }

  @Override
  public boolean isDictionaryEncoded() {
    return _dictionary != null;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public DataType getStoredType() {
    return _storedType;
  }

  @Override
  public void readDictIds(int[] docIds, int length, int[] dictIdBuffer, ForwardIndexReaderContext context) {
    if (_dictionary == null) {
      throw new UnsupportedOperationException("Dictionary not available for key: " + _key);
    }
    if (_dictIdReader != null) {
      // Fast path: O(1) per doc via bit-packed forward index
      for (int i = 0; i < length; i++) {
        dictIdBuffer[i] = _dictIdReader.readInt(docIds[i]);
      }
    } else {
      // Slow path: getString + indexOf for mutable segments
      for (int i = 0; i < length; i++) {
        String rawValue = _sparseMapIndexReader.getString(docIds[i], _key);
        if (rawValue == null || rawValue.isEmpty()) {
          dictIdBuffer[i] = _defaultDictId; // default value position in dictionary
        } else {
          dictIdBuffer[i] = _dictionary.indexOf(rawValue);
        }
      }
    }
  }

  @Override
  public int getInt(int docId, ForwardIndexReaderContext context) {
    return _sparseMapIndexReader.getInt(docId, _key);
  }

  @Override
  public long getLong(int docId, ForwardIndexReaderContext context) {
    return _sparseMapIndexReader.getLong(docId, _key);
  }

  @Override
  public float getFloat(int docId, ForwardIndexReaderContext context) {
    return _sparseMapIndexReader.getFloat(docId, _key);
  }

  @Override
  public double getDouble(int docId, ForwardIndexReaderContext context) {
    return _sparseMapIndexReader.getDouble(docId, _key);
  }

  @Override
  public String getString(int docId, ForwardIndexReaderContext context) {
    return _sparseMapIndexReader.getString(docId, _key);
  }

  @Override
  public byte[] getBytes(int docId, ForwardIndexReaderContext context) {
    return _sparseMapIndexReader.getBytes(docId, _key);
  }

  @Override
  public void close()
      throws IOException {
    // no-op: the underlying reader is owned by SparseMapDataSource
  }
}
