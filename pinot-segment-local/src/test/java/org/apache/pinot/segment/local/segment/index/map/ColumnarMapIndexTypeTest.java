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

import java.io.File;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ColumnarMapIndexTypeTest {

  private static final ColumnarMapIndexType INDEX_TYPE = ColumnarMapIndexPlugin.INSTANCE;
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();

  @Test
  public void testValidateRejectsNonMapColumn() {
    FieldSpec stringField = new DimensionFieldSpec("myCol", FieldSpec.DataType.STRING, true);
    FieldIndexConfigs configs = new FieldIndexConfigs.Builder()
        .add(INDEX_TYPE, MapIndexConfig.DEFAULT)
        .build();
    try {
      INDEX_TYPE.validate(configs, stringField, TABLE_CONFIG);
      fail("Expected IllegalStateException for non-MAP column");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("MAP"), "Error should mention MAP");
    }
  }

  @Test
  public void testValidateAcceptsMapColumn() {
    FieldSpec mapField = new ComplexFieldSpec("mapCol", FieldSpec.DataType.MAP, true);
    FieldIndexConfigs configs = new FieldIndexConfigs.Builder()
        .add(INDEX_TYPE, MapIndexConfig.DEFAULT)
        .build();
    // Should not throw
    INDEX_TYPE.validate(configs, mapField, TABLE_CONFIG);
  }

  @Test
  public void testValidateSkipsWhenDisabled() {
    FieldSpec stringField = new DimensionFieldSpec("myCol", FieldSpec.DataType.STRING, true);
    FieldIndexConfigs configs = new FieldIndexConfigs.Builder()
        .add(INDEX_TYPE, MapIndexConfig.DISABLED)
        .build();
    // Should not throw — disabled config skips validation
    INDEX_TYPE.validate(configs, stringField, TABLE_CONFIG);
  }

  @Test
  public void testDefaultConfigIsDisabled() {
    MapIndexConfig defaultConfig = INDEX_TYPE.getDefaultConfig();
    assertSame(defaultConfig, MapIndexConfig.DISABLED);
    assertTrue(defaultConfig.isDisabled());
  }

  @Test
  public void testHandlerIsNoOp() {
    IndexHandler handler = INDEX_TYPE.createIndexHandler(null, null, null, null);
    assertSame(handler, IndexHandler.NoOp.INSTANCE);
  }

  @Test
  public void testPrettyName() {
    assertEquals(INDEX_TYPE.getPrettyName(), "map");
  }

  @Test
  public void testFileExtensions() {
    assertEquals(INDEX_TYPE.getFileExtensions(null).size(), 1);
    assertEquals(INDEX_TYPE.getFileExtensions(null).get(0), ".map.idx");
  }

  @Test
  public void testCreateIndexCreatorReturnsSplitter() throws Exception {
    FieldSpec mapField = new ComplexFieldSpec("mapCol", FieldSpec.DataType.MAP, true);
    File tempDir = new File(System.getProperty("java.io.tmpdir"), "cmap_test_" + System.nanoTime());
    tempDir.mkdirs();
    try {
      IndexCreationContext.Common context = IndexCreationContext.builder()
          .withIndexDir(tempDir)
          .withFieldSpec(mapField)
          .withTotalDocs(100)
          .withDictionary(false)
          .build();
      IndexCreator creator = INDEX_TYPE.createIndexCreator(context, MapIndexConfig.DEFAULT);
      assertNotNull(creator);
      assertTrue(
          creator
              instanceof org.apache.pinot.segment.local.segment.creator.impl.columnarmap.ColumnarMapColumnSplitter);
      creator.close();
    } finally {
      org.apache.commons.io.FileUtils.deleteQuietly(tempDir);
    }
  }

  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testCreateMutableIndexThrows() {
    INDEX_TYPE.createMutableIndex(null, MapIndexConfig.DEFAULT);
  }
}
