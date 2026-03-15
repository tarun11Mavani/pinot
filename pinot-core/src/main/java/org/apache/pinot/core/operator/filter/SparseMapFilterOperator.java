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
package org.apache.pinot.core.operator.filter;

import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.docidsets.BitmapDocIdSet;
import org.apache.pinot.segment.spi.index.reader.SparseMapIndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Filter operator that uses SparseMapIndexReader for fast key-value lookups on SPARSE_MAP columns.
 * Supports EQ, NOT_EQ, IN, and NOT_IN predicates using the inverted index within the SparseMapIndex.
 */
public class SparseMapFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_SPARSE_MAP";

  private final SparseMapIndexReader _sparseMapReader;
  private final Predicate _predicate;
  private final String _keyName;

  public SparseMapFilterOperator(SparseMapIndexReader sparseMapReader, Predicate predicate, String keyName,
      int numDocs) {
    super(numDocs, false);
    _sparseMapReader = sparseMapReader;
    _predicate = predicate;
    _keyName = keyName;
  }

  @Override
  protected BlockDocIdSet getTrues() {
    ImmutableRoaringBitmap result = computeMatchingDocIds();
    if (result == null) {
      result = ImmutableRoaringBitmap.bitmapOf();
    }
    return new BitmapDocIdSet(result, _numDocs);
  }

  @Override
  public boolean canOptimizeCount() {
    return true;
  }

  @Override
  public int getNumMatchingDocs() {
    ImmutableRoaringBitmap result = computeMatchingDocIds();
    return result != null ? result.getCardinality() : 0;
  }

  @Override
  public boolean canProduceBitmaps() {
    return true;
  }

  @Override
  public BitmapCollection getBitmaps() {
    ImmutableRoaringBitmap result = computeMatchingDocIds();
    if (result == null) {
      result = ImmutableRoaringBitmap.bitmapOf();
    }
    return new BitmapCollection(_numDocs, false, result);
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME + "(key:" + _keyName + ",predicate:" + _predicate + ')';
  }

  private ImmutableRoaringBitmap computeMatchingDocIds() {
    switch (_predicate.getType()) {
      case EQ: {
        String value = ((EqPredicate) _predicate).getValue();
        return _sparseMapReader.getDocsWithKeyValue(_keyName, value);
      }
      // NOT_EQ semantics: docs where the key is ABSENT are excluded from results.
      // Only docs where the key is PRESENT and its value != the predicate value are returned.
      // This matches SQL NULL semantics: NULL != X is unknown, not true.
      case NOT_EQ: {
        String value = ((NotEqPredicate) _predicate).getValue();
        ImmutableRoaringBitmap matching = _sparseMapReader.getDocsWithKeyValue(_keyName, value);
        ImmutableRoaringBitmap presence = _sparseMapReader.getPresenceBitmap(_keyName);
        if (matching == null) {
          return presence;
        }
        return ImmutableRoaringBitmap.andNot(presence, matching);
      }
      case IN: {
        List<String> values = ((InPredicate) _predicate).getValues();
        MutableRoaringBitmap result = new MutableRoaringBitmap();
        for (String value : values) {
          ImmutableRoaringBitmap bitmap = _sparseMapReader.getDocsWithKeyValue(_keyName, value);
          if (bitmap != null) {
            result.or(bitmap);
          }
        }
        return result.toImmutableRoaringBitmap();
      }
      // NOT_IN semantics: same absence-exclusion policy as NOT_EQ.
      // Only docs where the key is present and value not in the set are returned.
      case NOT_IN: {
        List<String> values = ((NotInPredicate) _predicate).getValues();
        MutableRoaringBitmap excluded = new MutableRoaringBitmap();
        for (String value : values) {
          ImmutableRoaringBitmap bitmap = _sparseMapReader.getDocsWithKeyValue(_keyName, value);
          if (bitmap != null) {
            excluded.or(bitmap);
          }
        }
        ImmutableRoaringBitmap presence = _sparseMapReader.getPresenceBitmap(_keyName);
        return ImmutableRoaringBitmap.andNot(presence, excluded.toImmutableRoaringBitmap());
      }
      case IS_NOT_NULL:
        return _sparseMapReader.getPresenceBitmap(_keyName);

      case IS_NULL: {
        ImmutableRoaringBitmap presence = _sparseMapReader.getPresenceBitmap(_keyName);
        if (presence == null) {
          // Key never appears — all docs are NULL for this key
          MutableRoaringBitmap allDocs = new MutableRoaringBitmap();
          allDocs.add(0L, _numDocs);
          return allDocs.toImmutableRoaringBitmap();
        }
        return ImmutableRoaringBitmap.flip(presence, 0L, _numDocs);
      }

      default:
        return null;
    }
  }
}
