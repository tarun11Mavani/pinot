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
package org.apache.pinot.segment.local.segment.index.openstruct;

import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// {@link NullValueVectorReader} backed by an OPEN_STRUCT key's presence bitmap.
///
/// A document is null for a key when the key was never set on that document, i.e. when the
/// document is absent from the presence bitmap.
///
/// Reads the live presence bitmap rather than a snapshot: the bitmap is a
/// {@link ThreadSafeMutableRoaringBitmap} and ingestion only ever appends docIds >= the numDocs
/// captured by the enclosing DataSource, so the [0, numDocs) range this vector reports on is
/// already frozen. That also makes {@link #getNullBitmap()} cacheable — see there.
public class PresenceBasedNullValueVector implements NullValueVectorReader {
  private final ThreadSafeMutableRoaringBitmap _presenceBitmap;
  private final int _numDocs;
  private volatile ImmutableRoaringBitmap _nullBitmap;

  public PresenceBasedNullValueVector(ThreadSafeMutableRoaringBitmap presenceBitmap, int numDocs) {
    _presenceBitmap = presenceBitmap;
    _numDocs = numDocs;
  }

  @Override
  public boolean isNull(int docId) {
    return !_presenceBitmap.contains(docId);
  }

  /// Cached: callers re-derive this per block ({@link org.apache.pinot.core.operator.docvalsets.ProjectionBlockValSet}
  /// memoises only within a single block), and each rebuild clones the presence bitmap under the ingestion thread's
  /// monitor. Safe because [0, numDocs) is frozen — ingestion only appends docIds >= numDocs, and the only mutation
  /// of a key's presence bitmap is the append in `MutableKeyColumn#setValue`. A benign race may compute it twice;
  /// both results are equal and the field is volatile.
  @Override
  public ImmutableRoaringBitmap getNullBitmap() {
    ImmutableRoaringBitmap nullBitmap = _nullBitmap;
    if (nullBitmap == null) {
      MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
      if (_numDocs > 0) {
        bitmap.add(0L, _numDocs);
      }
      // getMutableRoaringBitmap() clones under the wrapper's monitor, so the andNot below iterates a
      // private copy that the ingestion thread cannot mutate mid-walk.
      bitmap.andNot(_presenceBitmap.getMutableRoaringBitmap());
      nullBitmap = bitmap;
      _nullBitmap = nullBitmap;
    }
    return nullBitmap;
  }
}
