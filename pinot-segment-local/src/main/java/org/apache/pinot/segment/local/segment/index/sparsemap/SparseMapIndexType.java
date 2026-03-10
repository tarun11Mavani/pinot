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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.SparseMapIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.SparseMapIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.SparseMapIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/**
 * Index type for SPARSE_MAP columns. Provides per-key columnar storage with presence bitmaps,
 * typed forward indexes, and optional inverted indexes for fast value-based filtering.
 */
public class SparseMapIndexType
    extends AbstractIndexType<SparseMapIndexConfig, SparseMapIndexReader, SparseMapIndexCreator> {
  public static final String INDEX_DISPLAY_NAME = "sparse_map";
  private static final List<String> EXTENSIONS =
      Collections.singletonList(V1Constants.Indexes.SPARSE_MAP_INDEX_FILE_EXTENSION);

  protected SparseMapIndexType() {
    super(StandardIndexes.SPARSE_MAP_ID);
  }

  @Override
  public Class<SparseMapIndexConfig> getIndexConfigClass() {
    return SparseMapIndexConfig.class;
  }

  @Override
  public SparseMapIndexConfig getDefaultConfig() {
    return SparseMapIndexConfig.DISABLED;
  }

  @Override
  public void validate(FieldIndexConfigs indexConfigs, FieldSpec fieldSpec, TableConfig tableConfig) {
    SparseMapIndexConfig config = indexConfigs.getConfig(this);
    if (config.isEnabled()) {
      String column = fieldSpec.getName();
      Preconditions.checkState(fieldSpec.isSingleValueField(),
          "Cannot create SparseMap index on multi-value column: %s", column);
      Preconditions.checkState(fieldSpec.getDataType() == FieldSpec.DataType.SPARSE_MAP,
          "SparseMap index can only be created on SPARSE_MAP columns, got: %s for column: %s",
          fieldSpec.getDataType(), column);
    }
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  public SparseMapIndexCreator createIndexCreator(IndexCreationContext context, SparseMapIndexConfig indexConfig)
      throws IOException {
    return new OnHeapSparseMapIndexCreator(context, indexConfig);
  }

  @Override
  protected IndexReaderFactory<SparseMapIndexReader> createReaderFactory() {
    return new IndexReaderFactory.Default<SparseMapIndexConfig, SparseMapIndexReader>() {
      @Override
      protected IndexType<SparseMapIndexConfig, SparseMapIndexReader, ?> getIndexType() {
        return SparseMapIndexType.this;
      }

      @Override
      protected SparseMapIndexReader createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata,
          SparseMapIndexConfig indexConfig)
          throws IOException, IndexReaderConstraintException {
        return new ImmutableSparseMapIndexReader(dataBuffer, metadata);
      }
    };
  }

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    return EXTENSIONS;
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      Schema schema, TableConfig tableConfig) {
    return new SparseMapIndexHandler(segmentDirectory, configsByCol, tableConfig, schema);
  }

  @Nullable
  @Override
  public MutableIndex createMutableIndex(MutableIndexContext context, SparseMapIndexConfig config) {
    if (!config.isEnabled()) {
      return null;
    }
    if (context.getFieldSpec().getFieldType() != FieldSpec.FieldType.SPARSE_MAP) {
      return null;
    }
    return new MutableSparseMapIndexImpl(context, config);
  }
}
