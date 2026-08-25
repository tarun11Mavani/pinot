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
package org.apache.pinot.core.operator.docidsets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.function.IntPredicate;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.operator.dociditerators.ScanBasedDocIdIterator;
import org.apache.pinot.segment.spi.Constants;
import org.roaringbitmap.BatchIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// When an AND has an index-based (bitmap) child and a scan-based child, [AndDocIdSet] restricts the scan to the
/// bitmap via [ScanBasedDocIdIterator#applyAnd], so the scan only reads the documents the index already narrowed down
/// to.
///
/// Wrapping the scans in an OR must not defeat that optimization: an OR whose children are all scan-based is itself
/// scan-based, because `(A OR B) AND docIds == (A AND docIds) OR (B AND docIds)`.
public class OrNestedScanApplyAndTest {
  private static final int NUM_DOCS = 100_000;

  @Test
  public void scansNestedInOrAreBoundedByTheAndBitmap() {
    // Models `col BETWEEN a AND b OR col BETWEEN c AND d` on a column with no usable index: two narrow, disjoint
    // windows in a large segment. Selectivity is what makes the unbounded path expensive, because advance() has to
    // walk from the current position all the way to the next matching document.
    CountingScanDocIdSet firstWindow = new CountingScanDocIdSet(docId -> docId >= 50_000 && docId <= 50_010);
    CountingScanDocIdSet secondWindow = new CountingScanDocIdSet(docId -> docId >= 90_000 && docId <= 90_010);

    // The index-based child narrows the AND down to 4 candidate documents; only two of them fall in a window.
    MutableRoaringBitmap indexDocIds = MutableRoaringBitmap.bitmapOf(7, 50_005, 90_003, 99_999);
    BlockDocIdSet indexDocIdSet = new BitmapDocIdSet(indexDocIds, NUM_DOCS);

    BlockDocIdSet orDocIdSet = new OrDocIdSet(Arrays.asList(firstWindow, secondWindow), NUM_DOCS);
    AndDocIdSet andDocIdSet = new AndDocIdSet(Arrays.asList(indexDocIdSet, orDocIdSet), null);

    assertEquals(drain(andDocIdSet.iterator()), Arrays.asList(50_005, 90_003));

    // Each scan should only read the 4 documents the index selected, never walk the 100000-document segment.
    long maxExpectedEntriesScanned = 2L * indexDocIds.getCardinality();
    long actualEntriesScanned = andDocIdSet.getNumEntriesScannedInFilter();
    assertTrue(actualEntriesScanned <= maxExpectedEntriesScanned,
        "Scans nested in an OR were not restricted to the AND bitmap: scanned " + actualEntriesScanned
            + " entries, expected at most " + maxExpectedEntriesScanned);
  }

  private static List<Integer> drain(BlockDocIdIterator iterator) {
    List<Integer> docIds = new ArrayList<>();
    for (int docId = iterator.next(); docId != Constants.EOF; docId = iterator.next()) {
      docIds.add(docId);
    }
    return docIds;
  }

  /// Mirrors the two cost regimes of a real scan-based doc id set: `applyAnd` only reads the documents it is handed,
  /// while `next`/`advance` walk the forward index document by document.
  private static final class CountingScanDocIdSet implements BlockDocIdSet {
    private final CountingScanDocIdIterator _iterator;

    CountingScanDocIdSet(IntPredicate matcher) {
      _iterator = new CountingScanDocIdIterator(matcher);
    }

    @Override
    public BlockDocIdIterator iterator() {
      return _iterator;
    }

    @Override
    public long getNumEntriesScannedInFilter() {
      return _iterator.getNumEntriesScanned();
    }
  }

  private static final class CountingScanDocIdIterator implements ScanBasedDocIdIterator {
    private final IntPredicate _matcher;

    private long _numEntriesScanned;
    private int _nextDocId;

    CountingScanDocIdIterator(IntPredicate matcher) {
      _matcher = matcher;
    }

    @Override
    public int next() {
      while (_nextDocId < NUM_DOCS) {
        int docId = _nextDocId++;
        _numEntriesScanned++;
        if (_matcher.test(docId)) {
          return docId;
        }
      }
      return Constants.EOF;
    }

    @Override
    public int advance(int targetDocId) {
      _nextDocId = targetDocId;
      return next();
    }

    @Override
    public MutableRoaringBitmap applyAnd(BatchIterator batchIterator, OptionalInt firstDoc, OptionalInt lastDoc) {
      MutableRoaringBitmap result = new MutableRoaringBitmap();
      int[] buffer = new int[BlockDocIdIterator.OPTIMAL_ITERATOR_BATCH_SIZE];
      while (batchIterator.hasNext()) {
        int limit = batchIterator.nextBatch(buffer);
        for (int i = 0; i < limit; i++) {
          _numEntriesScanned++;
          if (_matcher.test(buffer[i])) {
            result.add(buffer[i]);
          }
        }
      }
      return result;
    }

    @Override
    public long getNumEntriesScanned() {
      return _numEntriesScanned;
    }
  }
}
