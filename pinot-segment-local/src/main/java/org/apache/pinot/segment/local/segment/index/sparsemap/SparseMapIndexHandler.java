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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.local.segment.index.loader.BaseIndexHandler;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.SparseMapIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handles loading, adding and removing SparseMap indexes during segment reload.
 *
 * <p>Note: SparseMap indexes are created during segment ingestion only. This handler manages
 * removal of the index when the table config disables it. Creating a new SparseMap index
 * from an existing segment is not supported (requires re-ingestion).
 *
 * <p>Segment merge: when segments are merged (e.g., via the Minion merge task), the sparse map
 * index is rebuilt from scratch by re-ingesting all documents through
 * {@link OnHeapSparseMapIndexCreator}. The resulting merged index contains the union of all
 * keys observed across the merged segments, subject to the {@code maxKeys} limit configured
 * for the merged segment. No explicit merge handler is needed; the creator handles key-set
 * union naturally as documents are added in sequence.
 */
public class SparseMapIndexHandler extends BaseIndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SparseMapIndexHandler.class);

  private final Map<String, SparseMapIndexConfig> _sparseMapIndexConfigs;

  public SparseMapIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> fieldIndexConfigs,
      TableConfig tableConfig, Schema schema) {
    super(segmentDirectory, fieldIndexConfigs, tableConfig, schema);
    _sparseMapIndexConfigs =
        FieldIndexConfigsUtil.enableConfigByColumn(StandardIndexes.sparseMap(), _fieldIndexConfigs);
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_sparseMapIndexConfigs.keySet());
    Set<String> existingColumns =
        segmentReader.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.sparseMap());
    for (String column : existingColumns) {
      if (!columnsToAddIdx.contains(column)) {
        LOGGER.info("Need to remove existing SparseMap index from segment: {}, column: {}", segmentName, column);
        return true;
      }
    }
    return false;
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter) {
    String segmentName = _segmentDirectory.getSegmentMetadata().getName();
    Set<String> columnsToAddIdx = new HashSet<>(_sparseMapIndexConfigs.keySet());
    Set<String> existingColumns =
        segmentWriter.toSegmentDirectory().getColumnsWithIndex(StandardIndexes.sparseMap());
    for (String column : existingColumns) {
      if (!columnsToAddIdx.contains(column)) {
        LOGGER.info("Removing existing SparseMap index from segment: {}, column: {}", segmentName, column);
        segmentWriter.removeIndex(column, StandardIndexes.sparseMap());
        LOGGER.info("Removed SparseMap index from segment: {}, column: {}", segmentName, column);
      }
    }
    for (String column : columnsToAddIdx) {
      if (!existingColumns.contains(column)) {
        LOGGER.warn(
            "Cannot add SparseMap index to existing segment: {}, column: {}. Re-ingest the segment to create the "
                + "SparseMap index.", segmentName, column);
      }
    }
  }

  @Override
  public void postUpdateIndicesCleanup(SegmentDirectory.Writer segmentWriter) {
    // No temporary forward index to clean up for SparseMap indexes.
  }
}
