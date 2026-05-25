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
package org.apache.pinot.segment.local.segment.index.map;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.columnarmap.ColumnarMapColumnSplitter;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.creator.ColumnarMapIndexCreator;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext;
import org.apache.pinot.segment.spi.index.reader.ColumnarMapIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/**
 * Index type for MAP indexes on MAP columns.
 *
 * <p>The MAP index itself has no reader — per-key materialized columns are loaded by
 * standard {@code PhysicalColumnIndexContainer} and served through standard index readers.
 * This index type exists for SPI registration, config deserialization, and validation.
 */
public class ColumnarMapIndexType
    extends AbstractIndexType<MapIndexConfig, ColumnarMapIndexReader, ColumnarMapIndexCreator> {

  public static final String INDEX_DISPLAY_NAME = "map";
  private static final List<String> EXTENSIONS =
      Collections.singletonList(".map.idx");

  protected ColumnarMapIndexType() {
    super("map");
  }

  @Override
  public Class<MapIndexConfig> getIndexConfigClass() {
    return MapIndexConfig.class;
  }

  @Override
  public MapIndexConfig getDefaultConfig() {
    return MapIndexConfig.DISABLED;
  }

  @Override
  public void validate(FieldIndexConfigs indexConfigs, FieldSpec fieldSpec, TableConfig tableConfig) {
    MapIndexConfig config = indexConfigs.getConfig(this);
    if (config.isEnabled()) {
      String column = fieldSpec.getName();
      Preconditions.checkState(fieldSpec.getDataType() == FieldSpec.DataType.MAP,
          "MAP index can only be created on MAP columns, but column '%s' has type %s",
          column, fieldSpec.getDataType());
      Preconditions.checkState(fieldSpec.isSingleValueField(),
          "MAP index can only be created on single-value columns, but column '%s' is multi-value",
          column);
    }
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  protected ColumnConfigDeserializer<MapIndexConfig> createDeserializerForLegacyConfigs() {
    return IndexConfigDeserializer.fromIndexTypes(FieldConfig.IndexType.MAP,
        (tableConfig, fieldConfig) -> MapIndexConfig.fromProperties(fieldConfig.getProperties()));
  }

  @Override
  public ColumnarMapIndexCreator createIndexCreator(IndexCreationContext context, MapIndexConfig indexConfig) {
    FieldSpec fieldSpec = context.getFieldSpec();
    return new ColumnarMapColumnSplitter(context.getIndexDir(), fieldSpec.getName(), fieldSpec, indexConfig);
  }

  @Override
  protected IndexReaderFactory<ColumnarMapIndexReader> createReaderFactory() {
    return new NoOpReaderFactory();
  }

  @Override
  public List<String> getFileExtensions(@Nullable ColumnMetadata columnMetadata) {
    return EXTENSIONS;
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory,
      Map<String, FieldIndexConfigs> configsByCol, Schema schema, TableConfig tableConfig) {
    return IndexHandler.NoOp.INSTANCE;
  }

  @Nullable
  @Override
  public MutableIndex createMutableIndex(MutableIndexContext context, MapIndexConfig config) {
    throw new UnsupportedOperationException("Mutable MAP index is not yet supported");
  }

  /**
   * Reader factory that always returns null. The MAP index has no reader of its own —
   * materialized columns are loaded independently by the standard column loading infrastructure.
   */
  private static class NoOpReaderFactory implements IndexReaderFactory<ColumnarMapIndexReader> {
    @Nullable
    @Override
    public ColumnarMapIndexReader createIndexReader(SegmentDirectory.Reader segmentReader,
        FieldIndexConfigs fieldIndexConfigs, ColumnMetadata metadata) {
      return null;
    }
  }
}
