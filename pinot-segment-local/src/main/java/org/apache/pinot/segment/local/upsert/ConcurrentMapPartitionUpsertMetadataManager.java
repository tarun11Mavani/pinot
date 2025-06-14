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
package org.apache.pinot.segment.local.upsert;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentImpl;
import org.apache.pinot.segment.local.segment.readers.LazyRow;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/**
 * Implementation of {@link PartitionUpsertMetadataManager} that is backed by a {@link ConcurrentHashMap}.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@ThreadSafe
public class ConcurrentMapPartitionUpsertMetadataManager extends BasePartitionUpsertMetadataManager {

  // Used to initialize a reference to previous row for merging in partial upsert
  private final LazyRow _reusePreviousRow = new LazyRow();
  private final Map<String, Object> _reuseMergeResultHolder = new HashMap<>();

  @VisibleForTesting
  final ConcurrentHashMap<Object, RecordLocation> _primaryKeyToRecordLocationMap = new ConcurrentHashMap<>();

  public ConcurrentMapPartitionUpsertMetadataManager(String tableNameWithType, int partitionId, UpsertContext context) {
    super(tableNameWithType, partitionId, context);
  }

  @Override
  protected long getNumPrimaryKeys() {
    return _primaryKeyToRecordLocationMap.size();
  }

  @Override
  protected void doAddOrReplaceSegment(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator,
      @Nullable IndexSegment oldSegment, @Nullable MutableRoaringBitmap validDocIdsForOldSegment) {
    String segmentName = segment.getSegmentName();
    segment.enableUpsert(this, validDocIds, queryableDocIds);

    AtomicInteger numKeysInWrongSegment = new AtomicInteger();
    while (recordInfoIterator.hasNext()) {
      RecordInfo recordInfo = recordInfoIterator.next();
      int newDocId = recordInfo.getDocId();
      Comparable newComparisonValue = recordInfo.getComparisonValue();
      _primaryKeyToRecordLocationMap.compute(HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), _hashFunction),
          (primaryKey, currentRecordLocation) -> {
            if (currentRecordLocation != null) {
              // Existing primary key
              IndexSegment currentSegment = currentRecordLocation.getSegment();
              int currentDocId = currentRecordLocation.getDocId();
              int comparisonResult = newComparisonValue.compareTo(currentRecordLocation.getComparisonValue());

              // The current record is in the same segment
              // Update the record location when there is a tie to keep the newer record. Note that the record info
              // iterator will return records with incremental doc ids.
              if (currentSegment == segment) {
                if (comparisonResult >= 0) {
                  replaceDocId(segment, validDocIds, queryableDocIds, currentDocId, newDocId, recordInfo);
                  return new RecordLocation(segment, newDocId, newComparisonValue);
                } else {
                  return currentRecordLocation;
                }
              }

              // The current record is in an old segment being replaced
              // This could happen when committing a consuming segment, or reloading a completed segment. In this
              // case, we want to update the record location when there is a tie because the record locations should
              // point to the new added segment instead of the old segment being replaced. Also, do not update the valid
              // doc ids for the old segment because it has not been replaced yet. We pass in an optional valid doc ids
              // snapshot for the old segment, which can be updated and used to track the docs not replaced yet.
              if (currentSegment == oldSegment) {
                if (comparisonResult >= 0) {
                  if (validDocIdsForOldSegment == null && oldSegment != null && oldSegment.getValidDocIds() != null) {
                    // Update the old segment's bitmap in place if a copy of the bitmap was not provided.
                    replaceDocId(segment, validDocIds, queryableDocIds, oldSegment, currentDocId, newDocId, recordInfo);
                  } else {
                    addDocId(segment, validDocIds, queryableDocIds, newDocId, recordInfo);
                    if (validDocIdsForOldSegment != null) {
                      validDocIdsForOldSegment.remove(currentDocId);
                    }
                  }
                  return new RecordLocation(segment, newDocId, newComparisonValue);
                } else {
                  return currentRecordLocation;
                }
              }

              // This should not happen because the previously replaced segment should have all keys removed. We still
              // handle it here, and also track the number of keys not properly replaced previously.
              String currentSegmentName = currentSegment.getSegmentName();
              if (currentSegmentName.equals(segmentName)) {
                numKeysInWrongSegment.getAndIncrement();
                if (comparisonResult >= 0) {
                  addDocId(segment, validDocIds, queryableDocIds, newDocId, recordInfo);
                  return new RecordLocation(segment, newDocId, newComparisonValue);
                } else {
                  return currentRecordLocation;
                }
              }

              // The current record is in a different segment
              // Update the record location when getting a newer comparison value, or the value is the same as the
              // current value, but the segment has a larger sequence number (the segment is newer than the current
              // segment).
              if (comparisonResult > 0 || (comparisonResult == 0 && shouldReplaceOnComparisonTie(segmentName,
                  currentSegmentName, getAuthoritativeCreationTime(segment),
                  getAuthoritativeCreationTime(currentSegment)))) {
                replaceDocId(segment, validDocIds, queryableDocIds, currentSegment, currentDocId, newDocId, recordInfo);
                return new RecordLocation(segment, newDocId, newComparisonValue);
              } else {
                return currentRecordLocation;
              }
            } else {
              // New primary key
              addDocId(segment, validDocIds, queryableDocIds, newDocId, recordInfo);
              return new RecordLocation(segment, newDocId, newComparisonValue);
            }
          });
    }
    int numKeys = numKeysInWrongSegment.get();
    if (numKeys > 0) {
      _logger.warn("Found {} primary keys in the wrong segment when adding segment: {}", numKeys, segmentName);
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.UPSERT_KEYS_IN_WRONG_SEGMENT, numKeys);
    }
  }

  @Override
  protected void addSegmentWithoutUpsert(ImmutableSegmentImpl segment, ThreadSafeMutableRoaringBitmap validDocIds,
      @Nullable ThreadSafeMutableRoaringBitmap queryableDocIds, Iterator<RecordInfo> recordInfoIterator) {
    segment.enableUpsert(this, validDocIds, queryableDocIds);
    while (recordInfoIterator.hasNext()) {
      RecordInfo recordInfo = recordInfoIterator.next();
      int newDocId = recordInfo.getDocId();
      Comparable newComparisonValue = recordInfo.getComparisonValue();
      addDocId(segment, validDocIds, queryableDocIds, newDocId, recordInfo);
      _primaryKeyToRecordLocationMap.put(HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), _hashFunction),
          new RecordLocation(segment, newDocId, newComparisonValue));
    }
  }

  @Override
  protected void removeSegment(IndexSegment segment, Iterator<PrimaryKey> primaryKeyIterator) {
    while (primaryKeyIterator.hasNext()) {
      PrimaryKey primaryKey = primaryKeyIterator.next();
      _primaryKeyToRecordLocationMap.computeIfPresent(HashUtils.hashPrimaryKey(primaryKey, _hashFunction),
          (pk, recordLocation) -> {
            if (recordLocation.getSegment() == segment) {
              return null;
            }
            return recordLocation;
          });
    }
  }

  @Override
  public void doRemoveExpiredPrimaryKeys() {
    AtomicInteger numMetadataTTLKeysRemoved = new AtomicInteger();
    AtomicInteger numDeletedTTLKeysRemoved = new AtomicInteger();
    AtomicInteger numTotalKeysMarkForDeletion = new AtomicInteger();
    AtomicInteger numDeletedKeysWithinTTLWindow = new AtomicInteger();
    double largestSeenComparisonValue = _largestSeenComparisonValue.get();
    double metadataTTLKeysThreshold =
        _metadataTTL > 0 ? largestSeenComparisonValue - _metadataTTL : Double.NEGATIVE_INFINITY;
    double deletedKeysThreshold =
        _deletedKeysTTL > 0 ? largestSeenComparisonValue - _deletedKeysTTL : Double.NEGATIVE_INFINITY;
    _primaryKeyToRecordLocationMap.forEach((primaryKey, recordLocation) -> {
      double comparisonValue = ((Number) recordLocation.getComparisonValue()).doubleValue();
      if (_metadataTTL > 0 && comparisonValue < metadataTTLKeysThreshold) {
        _primaryKeyToRecordLocationMap.remove(primaryKey, recordLocation);
        numMetadataTTLKeysRemoved.getAndIncrement();
      } else if (_deletedKeysTTL > 0) {
        ThreadSafeMutableRoaringBitmap currentQueryableDocIds = recordLocation.getSegment().getQueryableDocIds();
        // if key not part of queryable doc id, it means it is deleted
        if (currentQueryableDocIds != null && !currentQueryableDocIds.contains(recordLocation.getDocId())) {
          numTotalKeysMarkForDeletion.getAndIncrement();
          if (comparisonValue >= deletedKeysThreshold) {
            // If key is within the TTL window, do not remove it from the primary hashmap
            numDeletedKeysWithinTTLWindow.getAndIncrement();
          } else {
            // delete key from primary hashmap
            _primaryKeyToRecordLocationMap.remove(primaryKey, recordLocation);
            removeDocId(recordLocation.getSegment(), recordLocation.getDocId());
            numDeletedTTLKeysRemoved.getAndIncrement();
          }
        }
      }
    });

    // Update metrics
    updatePrimaryKeyGauge();
    int numMetadataTTLKeys = numMetadataTTLKeysRemoved.get();
    if (numMetadataTTLKeys > 0) {
      _logger.info("Deleted {} primary keys based on metadataTTL", numMetadataTTLKeys);
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.METADATA_TTL_PRIMARY_KEYS_REMOVED,
          numMetadataTTLKeys);
    }
    int numDeletedTTLKeys = numDeletedTTLKeysRemoved.get();
    if (numDeletedTTLKeys > 0) {
      _logger.info("Deleted {} primary keys based on deletedKeysTTL", numDeletedTTLKeys);
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.DELETED_KEYS_TTL_PRIMARY_KEYS_REMOVED,
          numDeletedTTLKeys);
    }
    int numTotalKeysMarkedForDeletion = numTotalKeysMarkForDeletion.get();
    if (numTotalKeysMarkedForDeletion > 0) {
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.TOTAL_KEYS_MARKED_FOR_DELETION,
          numTotalKeysMarkedForDeletion);
    }
    int numDeletedKeysWithinTTLWindowValue = numDeletedKeysWithinTTLWindow.get();
    if (numDeletedKeysWithinTTLWindowValue > 0) {
      _serverMetrics.addMeteredTableValue(_tableNameWithType, ServerMeter.DELETED_KEYS_WITHIN_TTL_WINDOW,
          numDeletedKeysWithinTTLWindowValue);
    }
  }

  @Override
  protected boolean doAddRecord(MutableSegment segment, RecordInfo recordInfo) {
    AtomicBoolean isOutOfOrderRecord = new AtomicBoolean(false);
    ThreadSafeMutableRoaringBitmap validDocIds = Objects.requireNonNull(segment.getValidDocIds());
    ThreadSafeMutableRoaringBitmap queryableDocIds = segment.getQueryableDocIds();
    int newDocId = recordInfo.getDocId();
    Comparable newComparisonValue = recordInfo.getComparisonValue();

    // When TTL is enabled, update largestSeenComparisonValue when adding new record
    if (isTTLEnabled()) {
      double comparisonValue = ((Number) newComparisonValue).doubleValue();
      _largestSeenComparisonValue.getAndUpdate(v -> Math.max(v, comparisonValue));
    }

    _primaryKeyToRecordLocationMap.compute(HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), _hashFunction),
        (primaryKey, currentRecordLocation) -> {
          if (currentRecordLocation != null) {
            // Existing primary key

            // Update the record location when the new comparison value is greater than or equal to the current value.
            // Update the record location when there is a tie to keep the newer record.
            if (newComparisonValue.compareTo(currentRecordLocation.getComparisonValue()) >= 0) {
              IndexSegment currentSegment = currentRecordLocation.getSegment();
              int currentDocId = currentRecordLocation.getDocId();
              if (segment == currentSegment) {
                replaceDocId(segment, validDocIds, queryableDocIds, currentDocId, newDocId, recordInfo);
              } else {
                replaceDocId(segment, validDocIds, queryableDocIds, currentSegment, currentDocId, newDocId, recordInfo);
              }
              return new RecordLocation(segment, newDocId, newComparisonValue);
            } else {
              // Out-of-order record
              handleOutOfOrderEvent(currentRecordLocation.getComparisonValue(), recordInfo.getComparisonValue());
              isOutOfOrderRecord.set(true);
              return currentRecordLocation;
            }
          } else {
            // New primary key
            addDocId(segment, validDocIds, queryableDocIds, newDocId, recordInfo);
            return new RecordLocation(segment, newDocId, newComparisonValue);
          }
        });

    updatePrimaryKeyGauge();
    return !isOutOfOrderRecord.get();
  }

  @Override
  protected GenericRow doUpdateRecord(GenericRow record, RecordInfo recordInfo) {
    assert _partialUpsertHandler != null;
    _primaryKeyToRecordLocationMap.computeIfPresent(HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), _hashFunction),
        (pk, recordLocation) -> {
          // Read the previous record if the following conditions are met:
          // - New record is not a DELETE record
          // - New record is not out-of-order
          // - Previous record is not deleted
          if (!recordInfo.isDeleteRecord()
              && recordInfo.getComparisonValue().compareTo(recordLocation.getComparisonValue()) >= 0) {
            IndexSegment currentSegment = recordLocation.getSegment();
            ThreadSafeMutableRoaringBitmap currentQueryableDocIds = currentSegment.getQueryableDocIds();
            int currentDocId = recordLocation.getDocId();
            if (currentQueryableDocIds == null || currentQueryableDocIds.contains(currentDocId)) {
              _reusePreviousRow.init(currentSegment, currentDocId);
              _partialUpsertHandler.merge(_reusePreviousRow, record, _reuseMergeResultHolder);
              _reuseMergeResultHolder.clear();
            }
          }
          return recordLocation;
        });
    return record;
  }

  @VisibleForTesting
  static class RecordLocation {
    private final IndexSegment _segment;
    private final int _docId;
    private final Comparable _comparisonValue;

    public RecordLocation(IndexSegment indexSegment, int docId, Comparable comparisonValue) {
      _segment = indexSegment;
      _docId = docId;
      _comparisonValue = comparisonValue;
    }

    public IndexSegment getSegment() {
      return _segment;
    }

    public int getDocId() {
      return _docId;
    }

    public Comparable getComparisonValue() {
      return _comparisonValue;
    }
  }
}
