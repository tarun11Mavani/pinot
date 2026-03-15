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

import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * In-memory {@link Dictionary} implementation backed by sorted distinct values extracted from the
 * sparse map inverted index. Enables dictionary-based GROUP BY for sparse map key columns.
 *
 * <p>Values are stored as sorted strings and parsed on demand for typed access. The reverse map
 * provides O(1) string-to-dictId lookup. Since the source values come from a sorted inverted
 * index (TreeMap or sorted binary scan), the dictionary is always sorted.
 *
 * <p>Thread safety: this class is immutable after construction and safe for concurrent reads.
 */
public class SparseMapKeyDictionary implements Dictionary {

  private final DataType _valueType;
  private final String[] _sortedValues;
  private final Object2IntOpenHashMap<String> _valueToIdMap;

  public SparseMapKeyDictionary(DataType valueType, String[] sortedDistinctValues) {
    _valueType = valueType;
    _sortedValues = sortedDistinctValues;
    _valueToIdMap = new Object2IntOpenHashMap<>(sortedDistinctValues.length);
    _valueToIdMap.defaultReturnValue(NULL_VALUE_INDEX);
    for (int i = 0; i < sortedDistinctValues.length; i++) {
      _valueToIdMap.put(sortedDistinctValues[i], i);
    }
  }

  @Override
  public boolean isSorted() {
    return true;
  }

  @Override
  public DataType getValueType() {
    return _valueType;
  }

  @Override
  public int length() {
    return _sortedValues.length;
  }

  @Override
  public int indexOf(String stringValue) {
    return _valueToIdMap.getInt(stringValue);
  }

  @Override
  public int indexOf(int intValue) {
    return indexOf(String.valueOf(intValue));
  }

  @Override
  public int indexOf(long longValue) {
    return indexOf(String.valueOf(longValue));
  }

  @Override
  public int indexOf(float floatValue) {
    return indexOf(String.valueOf(floatValue));
  }

  @Override
  public int indexOf(double doubleValue) {
    return indexOf(String.valueOf(doubleValue));
  }

  @Override
  public int indexOf(BigDecimal bigDecimalValue) {
    return indexOf(bigDecimalValue.toPlainString());
  }

  @Override
  public int indexOf(ByteArray bytesValue) {
    return indexOf(bytesValue.toHexString());
  }

  @Override
  public int insertionIndexOf(String stringValue) {
    int index = _valueToIdMap.getInt(stringValue);
    if (index != NULL_VALUE_INDEX) {
      return index;
    }
    Comparator<String> cmp = getComparator(_valueType);
    if (cmp != null) {
      return Arrays.binarySearch(_sortedValues, stringValue, cmp);
    }
    return Arrays.binarySearch(_sortedValues, stringValue);
  }

  @Override
  public IntSet getDictIdsInRange(String lower, String upper, boolean includeLower, boolean includeUpper) {
    // Sorted dictionary — this method should not be called
    throw new UnsupportedOperationException();
  }

  @Override
  public int compare(int dictId1, int dictId2) {
    return Integer.compare(dictId1, dictId2);
  }

  @Override
  public Comparable getMinVal() {
    if (_sortedValues.length == 0) {
      return null;
    }
    return parseValue(_sortedValues[0]);
  }

  @Override
  public Comparable getMaxVal() {
    if (_sortedValues.length == 0) {
      return null;
    }
    return parseValue(_sortedValues[_sortedValues.length - 1]);
  }

  @Override
  public Object getSortedValues() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(int dictId) {
    return parseValue(_sortedValues[dictId]);
  }

  @Override
  public int getIntValue(int dictId) {
    return Integer.parseInt(_sortedValues[dictId]);
  }

  @Override
  public long getLongValue(int dictId) {
    return Long.parseLong(_sortedValues[dictId]);
  }

  @Override
  public float getFloatValue(int dictId) {
    return Float.parseFloat(_sortedValues[dictId]);
  }

  @Override
  public double getDoubleValue(int dictId) {
    return Double.parseDouble(_sortedValues[dictId]);
  }

  @Override
  public BigDecimal getBigDecimalValue(int dictId) {
    return new BigDecimal(_sortedValues[dictId]);
  }

  @Override
  public String getStringValue(int dictId) {
    return _sortedValues[dictId];
  }

  @Override
  public byte[] getBytesValue(int dictId) {
    return _sortedValues[dictId].getBytes(java.nio.charset.StandardCharsets.UTF_8);
  }

  @Override
  public void close()
      throws IOException {
    // no-op: in-memory dictionary
  }

  private Comparable parseValue(String stringValue) {
    switch (_valueType) {
      case INT:
        return Integer.parseInt(stringValue);
      case LONG:
        return Long.parseLong(stringValue);
      case FLOAT:
        return Float.parseFloat(stringValue);
      case DOUBLE:
        return Double.parseDouble(stringValue);
      default:
        return stringValue;
    }
  }

  /**
   * Returns a numeric comparator for the given type, or null for STRING/BYTES (lexicographic).
   */
  private static Comparator<String> getComparator(DataType valueType) {
    switch (valueType) {
      case INT:
        return Comparator.comparingInt(Integer::parseInt);
      case LONG:
        return Comparator.comparingLong(Long::parseLong);
      case FLOAT:
        return (a, b) -> Float.compare(Float.parseFloat(a), Float.parseFloat(b));
      case DOUBLE:
        return Comparator.comparingDouble(Double::parseDouble);
      default:
        return null;
    }
  }
}
