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
package org.apache.pinot.segment.local.segment.creator.impl.columnarmap;

import java.io.File;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MapNaming;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class ColumnarMapColumnSplitterTest {

  private File _indexDir;

  @BeforeMethod
  public void setUp() {
    _indexDir = new File(FileUtils.getTempDirectory(),
        "ColumnarMapColumnSplitterTest_" + System.nanoTime());
    _indexDir.mkdirs();
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_indexDir);
  }

  @Test
  public void testDenseStringWithDictionary()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setValueFieldSpecs(Map.of(
        "country", new DimensionFieldSpec("country", DataType.STRING, true)));
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    MapIndexConfig config = new MapIndexConfig(true, false, null, null, 100,
        Set.of("country"), 0.5);

    ColumnarMapColumnSplitter splitter = new ColumnarMapColumnSplitter(_indexDir, "metrics",
        fieldSpec, config);
    splitter.add(Map.of("country", "US"), 0);
    splitter.add(Map.of("country", "UK"), 0);
    splitter.add(Map.of("country", "US"), 0);
    splitter.add(Map.of("country", "DE"), 0);
    splitter.seal();
    splitter.close();

    assertTrue(splitter.getResolvedDenseKeys().contains("country"));

    String materializedCol = MapNaming.materializedColumnName("metrics", "country");
    Map<String, PropertiesConfiguration> metadata = splitter.getMaterializedColumnMetadata();
    assertTrue(metadata.containsKey(materializedCol));

    PropertiesConfiguration props = metadata.get(materializedCol);
    assertEquals(props.getString(
        V1Constants.MetadataKeys.Column.getKeyFor(materializedCol,
            V1Constants.MetadataKeys.Column.DATA_TYPE)), "STRING");
    assertNotNull(props.getString(
        V1Constants.MetadataKeys.Column.getKeyFor(materializedCol,
            V1Constants.MetadataKeys.Column.PARENT_MAP_COLUMN)));
    assertEquals(props.getString(
        V1Constants.MetadataKeys.Column.getKeyFor(materializedCol,
            V1Constants.MetadataKeys.Column.PARENT_MAP_COLUMN)), "metrics");

    // Dictionary file should exist
    File dictFile = new File(_indexDir,
        materializedCol + V1Constants.Dict.FILE_EXTENSION);
    assertTrue(dictFile.exists(), "Dictionary file should exist for dense STRING key");

    // Forward index file should exist
    File fwdFile = new File(_indexDir,
        materializedCol + V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION);
    assertTrue(fwdFile.exists(), "Forward index should exist for dense key");
  }

  @Test
  public void testDenseLongRaw()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setValueFieldSpecs(Map.of(
        "clicks", new DimensionFieldSpec("clicks", DataType.LONG, true)));
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    MapIndexConfig config = new MapIndexConfig(true, false, null,
        Set.of("clicks"), 100, Set.of("clicks"), 0.5);

    ColumnarMapColumnSplitter splitter = new ColumnarMapColumnSplitter(_indexDir, "metrics",
        fieldSpec, config);
    for (int i = 0; i < 100; i++) {
      splitter.add(Map.of("clicks", (long) i), 0);
    }
    splitter.seal();
    splitter.close();

    String materializedCol = MapNaming.materializedColumnName("metrics", "clicks");
    Map<String, PropertiesConfiguration> metadata = splitter.getMaterializedColumnMetadata();
    assertTrue(metadata.containsKey(materializedCol));

    PropertiesConfiguration props = metadata.get(materializedCol);
    assertEquals(props.getString(
        V1Constants.MetadataKeys.Column.getKeyFor(materializedCol,
            V1Constants.MetadataKeys.Column.DATA_TYPE)), "LONG");
    assertFalse(props.getBoolean(
        V1Constants.MetadataKeys.Column.getKeyFor(materializedCol,
            V1Constants.MetadataKeys.Column.HAS_DICTIONARY)));
  }

  @Test
  public void testSparseJsonColumn()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    // Set fill rate to 1.0 so no key qualifies as dense automatically
    MapIndexConfig config = new MapIndexConfig(true, false, null, null, 100,
        null, 1.0);

    ColumnarMapColumnSplitter splitter = new ColumnarMapColumnSplitter(_indexDir, "metrics",
        fieldSpec, config);
    splitter.add(Map.of("rare_key_1", "value1"), 0);
    splitter.add(Map.of("rare_key_2", "value2"), 0);
    splitter.add(Map.of("rare_key_3", "value3"), 0);
    splitter.seal();
    splitter.close();

    assertTrue(splitter.getResolvedDenseKeys().isEmpty());

    String sparseCol = MapNaming.sparseColumnName("metrics");
    Map<String, PropertiesConfiguration> metadata = splitter.getMaterializedColumnMetadata();
    assertTrue(metadata.containsKey(sparseCol));

    PropertiesConfiguration props = metadata.get(sparseCol);
    assertEquals(props.getString(
        V1Constants.MetadataKeys.Column.getKeyFor(sparseCol,
            V1Constants.MetadataKeys.Column.DATA_TYPE)), "STRING");
    assertNotNull(props.getString(
        V1Constants.MetadataKeys.Column.getKeyFor(sparseCol,
            V1Constants.MetadataKeys.Column.PARENT_MAP_COLUMN)));
  }

  @Test
  public void testMixedTierAssignment()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    // 0.3 fill rate threshold — country appears in 4/5 docs (0.8), rare_key in 1/5 (0.2)
    MapIndexConfig config = new MapIndexConfig(true, false, null, null, 100,
        null, 0.3);

    ColumnarMapColumnSplitter splitter = new ColumnarMapColumnSplitter(_indexDir, "metrics",
        fieldSpec, config);
    splitter.add(Map.of("country", "US", "rare_key", "x"), 0);
    splitter.add(Map.of("country", "UK"), 0);
    splitter.add(Map.of("country", "DE"), 0);
    splitter.add(Map.of("country", "FR"), 0);
    splitter.add(Map.of(), 0);
    splitter.seal();
    splitter.close();

    assertTrue(splitter.getResolvedDenseKeys().contains("country"));
    assertFalse(splitter.getResolvedDenseKeys().contains("rare_key"));

    Map<String, PropertiesConfiguration> metadata = splitter.getMaterializedColumnMetadata();
    assertTrue(metadata.containsKey(MapNaming.materializedColumnName("metrics", "country")));
    assertTrue(metadata.containsKey(MapNaming.sparseColumnName("metrics")));
  }

  @Test
  public void testMaxKeysEnforced()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    MapIndexConfig config = new MapIndexConfig(true, false, null, null, 2,
        null, 0.0);

    ColumnarMapColumnSplitter splitter = new ColumnarMapColumnSplitter(_indexDir, "metrics",
        fieldSpec, config);
    splitter.add(Map.of("key1", "a", "key2", "b", "key3", "c"), 0);
    splitter.seal();
    splitter.close();

    // All 3 keys buffered, but only 2 become dense (top 2 by fillRate, lex tiebreak: key1, key2)
    Set<String> dense = splitter.getResolvedDenseKeys();
    assertEquals(dense.size(), 2);
    assertTrue(dense.contains("key1"));
    assertTrue(dense.contains("key2"));
    assertFalse(dense.contains("key3"));

    // key3 goes to sparse column; dense keys + sparse = 3 metadata entries
    Map<String, PropertiesConfiguration> metadata = splitter.getMaterializedColumnMetadata();
    assertEquals(metadata.size(), 3);
    assertTrue(metadata.containsKey(MapNaming.sparseColumnName("metrics")));
  }

  @Test
  public void testEmptyMapDocs()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    MapIndexConfig config = new MapIndexConfig(true, false, null, null, 100,
        Set.of("country"), 0.5);

    ColumnarMapColumnSplitter splitter = new ColumnarMapColumnSplitter(_indexDir, "metrics",
        fieldSpec, config);
    splitter.add(Map.of("country", "US"), 0);
    splitter.add(Map.of(), 0);
    splitter.add(null, 0);
    splitter.add(Map.of("country", "UK"), 0);
    splitter.seal();
    splitter.close();

    String materializedCol = MapNaming.materializedColumnName("metrics", "country");
    Map<String, PropertiesConfiguration> metadata = splitter.getMaterializedColumnMetadata();
    assertNotNull(metadata.get(materializedCol));
  }

  @Test
  public void testNumericDictOrdering()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("m", DataType.MAP, true);
    fieldSpec.setValueFieldSpecs(Map.of(
        "score", new DimensionFieldSpec("score", DataType.INT, true)));
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    MapIndexConfig config = new MapIndexConfig(true, false,
        Set.of("score"), null, 100, Set.of("score"), 0.0);

    ColumnarMapColumnSplitter splitter = new ColumnarMapColumnSplitter(_indexDir, "m",
        fieldSpec, config);

    // Insert values that sort differently as strings vs ints:
    // String sort: "100" < "11" < "2" < "9"
    // Int sort: 2 < 9 < 11 < 100
    splitter.add(Map.of("score", 9), 0);
    splitter.add(Map.of("score", 100), 1);
    splitter.add(Map.of("score", 2), 2);
    splitter.add(Map.of("score", 11), 3);

    splitter.seal();
    splitter.close();

    String materializedCol = MapNaming.materializedColumnName("m", "score");
    File dictFile = new File(_indexDir, materializedCol + V1Constants.Dict.FILE_EXTENSION);
    assertTrue(dictFile.exists(), "Dictionary file should exist");

    Map<String, PropertiesConfiguration> meta = splitter.getMaterializedColumnMetadata();
    PropertiesConfiguration props = meta.get(materializedCol);
    assertNotNull(props);
    assertEquals(props.getString(V1Constants.MetadataKeys.Column.getKeyFor(materializedCol,
        V1Constants.MetadataKeys.Column.DATA_TYPE)), "INT");

    // Verify min/max are numeric, not lexicographic.
    // min/max are computed from actual data values (not including default).
    // Lexicographic sort would give max="9", but numeric sort gives max=100.
    String minVal = props.getString(V1Constants.MetadataKeys.Column.getKeyFor(materializedCol,
        V1Constants.MetadataKeys.Column.MIN_VALUE));
    String maxVal = props.getString(V1Constants.MetadataKeys.Column.getKeyFor(materializedCol,
        V1Constants.MetadataKeys.Column.MAX_VALUE));
    assertEquals(minVal, "2", "Min should be numeric 2, not lex '100'");
    assertEquals(maxVal, "100", "Max should be numeric 100, not lex '9'");
  }

  @Test
  public void testDenseSetSelectedByFillRateNotInsertionOrder()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("m", DataType.MAP, true);
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    // maxKeys=2: only 2 keys can be dense
    MapIndexConfig config = new MapIndexConfig(true, false, null, null, 2,
        null, 0.0);

    ColumnarMapColumnSplitter splitter = new ColumnarMapColumnSplitter(_indexDir, "m",
        fieldSpec, config);

    // Key "rare" appears first (in doc 0 only) -- 10% fill rate
    // Keys "freq_a" and "freq_b" appear later but in 9/10 docs -- 90% fill rate
    splitter.add(Map.of("rare", "v"), 0);
    for (int i = 1; i < 10; i++) {
      splitter.add(Map.of("freq_a", "v", "freq_b", "v"), i);
    }

    splitter.seal();

    Set<String> dense = splitter.getResolvedDenseKeys();
    // "freq_a" and "freq_b" should win because they have higher fill rate
    assertTrue(dense.contains("freq_a"), "freq_a (90% fill) should be dense");
    assertTrue(dense.contains("freq_b"), "freq_b (90% fill) should be dense");
    assertFalse(dense.contains("rare"), "rare (10% fill) should be sparse, not dense");
  }

  @Test
  public void testInvertedIndexCreatedWhenConfigured()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setValueFieldSpecs(Map.of(
        "country", new DimensionFieldSpec("country", DataType.STRING, true)));
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    MapIndexConfig config = new MapIndexConfig(true, false,
        Set.of("country"), null, 100, Set.of("country"), 0.5);

    ColumnarMapColumnSplitter splitter = new ColumnarMapColumnSplitter(_indexDir, "metrics",
        fieldSpec, config);
    splitter.add(Map.of("country", "US"), 0);
    splitter.add(Map.of("country", "UK"), 0);
    splitter.add(Map.of("country", "US"), 0);
    splitter.seal();
    splitter.close();

    String materializedCol = MapNaming.materializedColumnName("metrics", "country");
    File invFile = new File(_indexDir,
        materializedCol + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION);
    assertTrue(invFile.exists(), "Inverted index file should exist for configured key");
  }

  @Test
  public void testDenseBytesKey()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setValueFieldSpecs(Map.of(
        "payload", new DimensionFieldSpec("payload", DataType.BYTES, true)));
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.BYTES, true));

    MapIndexConfig config = new MapIndexConfig(true, false, null, null, 100,
        Set.of("payload"), 0.5);

    ColumnarMapColumnSplitter splitter = new ColumnarMapColumnSplitter(_indexDir, "metrics",
        fieldSpec, config);
    // byte[] is not Comparable; the seal-time min/max loop must wrap it in ByteArray.
    splitter.add(Map.of("payload", new byte[]{0x01, 0x02}), 0);
    splitter.add(Map.of("payload", new byte[]{0x03, 0x04}), 1);
    splitter.add(Map.of("payload", new byte[]{0x00, 0x01}), 2);
    splitter.seal();
    splitter.close();

    assertTrue(splitter.getResolvedDenseKeys().contains("payload"));
    String materializedCol = MapNaming.materializedColumnName("metrics", "payload");
    PropertiesConfiguration props = splitter.getMaterializedColumnMetadata().get(materializedCol);
    assertNotNull(props);
    assertEquals(props.getString(
        V1Constants.MetadataKeys.Column.getKeyFor(materializedCol,
            V1Constants.MetadataKeys.Column.MIN_VALUE)), "0001");
    assertEquals(props.getString(
        V1Constants.MetadataKeys.Column.getKeyFor(materializedCol,
            V1Constants.MetadataKeys.Column.MAX_VALUE)), "0304");
  }

  @Test
  public void testKeysWithDoubleUnderscores()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    MapIndexConfig config = new MapIndexConfig(true, false, null, null, 100, null, 0.0);

    ColumnarMapColumnSplitter splitter = new ColumnarMapColumnSplitter(_indexDir, "metrics",
        fieldSpec, config);
    splitter.add(Map.of("user__name", "alice", "click__count__total", "99"), 0);
    splitter.add(Map.of("user__name", "bob"), 0);
    splitter.add(Map.of("simple_key", "val"), 0);
    splitter.seal();
    splitter.close();

    assertTrue(splitter.getResolvedDenseKeys().contains("user__name"));
    assertTrue(splitter.getResolvedDenseKeys().contains("click__count__total"));
    assertTrue(splitter.getResolvedDenseKeys().contains("simple_key"));

    String materializedCol = MapNaming.materializedColumnName("metrics", "user__name");
    Map<String, PropertiesConfiguration> metadata = splitter.getMaterializedColumnMetadata();
    assertTrue(metadata.containsKey(materializedCol));
  }
}
