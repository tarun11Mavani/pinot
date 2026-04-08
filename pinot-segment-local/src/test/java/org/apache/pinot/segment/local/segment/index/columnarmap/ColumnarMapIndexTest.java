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
package org.apache.pinot.segment.local.segment.index.columnarmap;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.reader.ColumnarMapIndexReader;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.ColumnarMapIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Unit tests for {@link OnHeapColumnarMapIndexCreator} and {@link ImmutableColumnarMapIndexReader}.
 */
public class ColumnarMapIndexTest {

  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "ColumnarMapIndexTest");
  private static final String COLUMN_NAME = "sparseCol";

  @BeforeMethod
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  // ---- Helper to build a ComplexFieldSpec for MAP ----

  private static ComplexFieldSpec buildMapFieldSpec(String columnName) {
    Map<String, FieldSpec> childFieldSpecs = Map.of(
        "key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
        "value", new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true)
    );
    return new ComplexFieldSpec(columnName, FieldSpec.DataType.MAP, true, childFieldSpecs);
  }

  // ---- Helper to create index file ----

  private File createIndex(Map<String, FieldSpec.DataType> keyTypes, Map<String, Object>[] docs)
      throws IOException {
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 1000);
    return createIndex(fieldSpec, config, keyTypes, docs);
  }

  private File createIndex(ComplexFieldSpec fieldSpec, ColumnarMapIndexConfig config, Map<String, Object>[] docs)
      throws IOException {
    return createIndex(fieldSpec, config, null, docs);
  }

  private File createIndex(ComplexFieldSpec fieldSpec, ColumnarMapIndexConfig config,
      Map<String, FieldSpec.DataType> keyTypes, Map<String, Object>[] docs)
      throws IOException {
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    try (OnHeapColumnarMapIndexCreator creator =
        new OnHeapColumnarMapIndexCreator(INDEX_DIR, COLUMN_NAME, fieldSpec, config,
            keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    assertTrue(indexFile.exists(), "Index file should exist after sealing");
    return indexFile;
  }

  @Test
  public void testBasicIntAndStringKeys()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("age", FieldSpec.DataType.INT);
    keyTypes.put("name", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("age", 25, "name", "alice"),
        Map.of("age", 30),
        Map.of("name", "charlie"),
        Map.of("age", 25, "name", "dave")
    };

    File indexFile = createIndex(keyTypes, docs);

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      // Verify keys
      assertTrue(reader.getKeys().contains("age"));
      assertTrue(reader.getKeys().contains("name"));

      // Verify key types
      assertEquals(reader.getKeyValueType("age"), FieldSpec.DataType.INT);
      assertEquals(reader.getKeyValueType("name"), FieldSpec.DataType.STRING);

      // Verify presence bitmaps
      ImmutableRoaringBitmap ageBitmap = reader.getPresenceBitmap("age");
      assertTrue(ageBitmap.contains(0));
      assertTrue(ageBitmap.contains(1));
      assertFalse(ageBitmap.contains(2));
      assertTrue(ageBitmap.contains(3));

      ImmutableRoaringBitmap nameBitmap = reader.getPresenceBitmap("name");
      assertTrue(nameBitmap.contains(0));
      assertFalse(nameBitmap.contains(1));
      assertTrue(nameBitmap.contains(2));
      assertTrue(nameBitmap.contains(3));

      // Verify typed values
      assertEquals(reader.getInt(0, "age"), 25);
      assertEquals(reader.getInt(1, "age"), 30);
      assertEquals(reader.getInt(3, "age"), 25);

      assertEquals(reader.getString(0, "name"), "alice");
      assertEquals(reader.getString(2, "name"), "charlie");
      assertEquals(reader.getString(3, "name"), "dave");

      // Verify doc counts
      assertEquals(reader.getNumDocsWithKey("age"), 3);
      assertEquals(reader.getNumDocsWithKey("name"), 3);
    }
  }

  @Test
  public void testGetMap()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("score", FieldSpec.DataType.DOUBLE);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("score", 1.5, "tag", "a"),
        Map.of("score", 2.0),
        new HashMap<>()
    };

    File indexFile = createIndex(keyTypes, docs);

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      Map<String, Object> doc0 = reader.getMap(0);
      assertTrue(doc0.containsKey("score"));
      assertEquals((double) doc0.get("score"), 1.5, 0.001);
      assertTrue(doc0.containsKey("tag")); // stored as STRING (default)
      assertEquals(doc0.get("tag"), "a");

      Map<String, Object> doc1 = reader.getMap(1);
      assertTrue(doc1.containsKey("score"));
      assertEquals((double) doc1.get("score"), 2.0, 0.001);
      assertFalse(doc1.containsKey("tag"));

      Map<String, Object> doc2 = reader.getMap(2);
      assertTrue(doc2.isEmpty());
    }
  }

  @Test
  public void testAbsentKeyReturnsEmptyPresenceBitmap()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("x", FieldSpec.DataType.INT);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{Map.of("x", 1)};

    File indexFile = createIndex(keyTypes, docs);

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      ImmutableRoaringBitmap missingBitmap = reader.getPresenceBitmap("nonexistent");
      assertTrue(missingBitmap.isEmpty());

      assertNull(reader.getKeyValueType("nonexistent"));
      assertEquals(reader.getNumDocsWithKey("nonexistent"), 0);
    }
  }

  @Test
  public void testWithInvertedIndex()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("color", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("color", "red"),
        Map.of("color", "blue"),
        Map.of("color", "red"),
        Map.of("color", "green")
    };

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    try (OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(
        INDEX_DIR, COLUMN_NAME, fieldSpec, config, keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      ImmutableRoaringBitmap reds = reader.getDocsWithKeyValue("color", "red");
      assertNotNull(reds);
      assertTrue(reds.contains(0));
      assertFalse(reds.contains(1));
      assertTrue(reds.contains(2));
      assertFalse(reds.contains(3));

      ImmutableRoaringBitmap blues = reader.getDocsWithKeyValue("color", "blue");
      assertNotNull(blues);
      assertFalse(blues.contains(0));
      assertTrue(blues.contains(1));

      ImmutableRoaringBitmap nullResult = reader.getDocsWithKeyValue("color", "purple");
      assertNull(nullResult);
    }
  }

  @Test
  public void testMutableIndexBasicOperations()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("price", FieldSpec.DataType.FLOAT);
    keyTypes.put("brand", FieldSpec.DataType.STRING);

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 100);

    try (MutableColumnarMapIndexImpl mutableIndex = new MutableColumnarMapIndexImpl(
        buildMutableContext(fieldSpec), config, keyTypes, FieldSpec.DataType.STRING)) {

      mutableIndex.add(Map.of("price", 9.99f, "brand", "acme"), -1, 0);
      mutableIndex.add(Map.of("price", 14.99f), -1, 1);
      mutableIndex.add(Map.of("brand", "foo"), -1, 2);
      mutableIndex.add(Map.of("price", 9.99f, "brand", "acme"), -1, 3);

      // Test presence bitmaps
      ImmutableRoaringBitmap priceBitmap = mutableIndex.getPresenceBitmap("price");
      assertTrue(priceBitmap.contains(0));
      assertTrue(priceBitmap.contains(1));
      assertFalse(priceBitmap.contains(2));
      assertTrue(priceBitmap.contains(3));

      ImmutableRoaringBitmap brandBitmap = mutableIndex.getPresenceBitmap("brand");
      assertTrue(brandBitmap.contains(0));
      assertFalse(brandBitmap.contains(1));
      assertTrue(brandBitmap.contains(2));
      assertTrue(brandBitmap.contains(3));

      // Test typed reads
      assertEquals(mutableIndex.getFloat(0, "price"), 9.99f, 0.001f);
      assertEquals(mutableIndex.getFloat(1, "price"), 14.99f, 0.001f);
      assertEquals(mutableIndex.getString(0, "brand"), "acme");
      assertEquals(mutableIndex.getString(2, "brand"), "foo");

      // Test getString on numeric key — must not throw ClassCastException
      assertEquals(mutableIndex.getString(0, "price"), "9.99");
      assertEquals(mutableIndex.getString(1, "price"), "14.99");

      // Test getMap
      Map<String, Object> doc0 = mutableIndex.getMap(0);
      assertEquals(doc0.get("brand"), "acme");

      // Test getNumDocsWithKey
      assertEquals(mutableIndex.getNumDocsWithKey("price"), 3);
      assertEquals(mutableIndex.getNumDocsWithKey("brand"), 3);
      assertEquals(mutableIndex.getNumDocsWithKey("nonexistent"), 0);
    }
  }

  @Test
  public void testMaxKeysDropsExcessKeysImmutable()
      throws Exception {
    String colName = "maxkeys_imm_test";
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(colName);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 2); // maxKeys=2
    OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(INDEX_DIR, colName, fieldSpec, config);

    // Add a doc with 3 keys — only 2 should be stored
    Map<String, Object> doc = new HashMap<>();
    doc.put("alpha", 1);
    doc.put("beta", 2);
    doc.put("gamma", 3);
    creator.add(doc);
    creator.seal();
    creator.close();

    File indexFile = new File(INDEX_DIR, colName + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buf, null);

    assertEquals(2, reader.getKeys().size());
    buf.close();
  }

  @Test
  public void testMaxKeysDropsExcessKeysMutable()
      throws Exception {
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 2); // maxKeys=2
    ComplexFieldSpec fieldSpec = buildMapFieldSpec("mutable_maxkeys_test");

    try (MutableColumnarMapIndexImpl mutableIndex = new MutableColumnarMapIndexImpl(
        buildMutableContext(fieldSpec), config)) {

      // Add doc 0 with 3 keys — only 2 should be stored
      Map<String, Object> doc = new HashMap<>();
      doc.put("alpha", 1);
      doc.put("beta", 2);
      doc.put("gamma", 3);
      mutableIndex.add(doc, -1, 0);

      assertEquals(2, mutableIndex.getKeys().size());
    }
  }

  @Test
  public void testNullValueTreatedAsAbsentImmutable()
      throws Exception {
    String colName = "null_absent_imm_test";
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(colName);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 10);
    OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(INDEX_DIR, colName, fieldSpec, config);

    Map<String, Object> doc = new HashMap<>();
    doc.put("key", null);   // explicit null — must be treated as absent
    creator.add(doc);
    creator.seal();
    creator.close();

    File indexFile = new File(INDEX_DIR, colName + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buf, null);

    // Key must not appear in the index at all
    assertFalse(reader.getPresenceBitmap("key").contains(0));
    assertEquals(0, reader.getKeys().size());
    buf.close();
  }

  @Test
  public void testNullValueTreatedAsAbsentMutable()
      throws Exception {
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 10);
    ComplexFieldSpec fieldSpec = buildMapFieldSpec("mutable_null_absent_test");

    try (MutableColumnarMapIndexImpl mutableIndex = new MutableColumnarMapIndexImpl(
        buildMutableContext(fieldSpec), config)) {

      // add doc 0 with Map {"key": null}
      Map<String, Object> doc = new HashMap<>();
      doc.put("key", null);
      mutableIndex.add(doc, -1, 0);

      // key must not appear in the presence bitmap or key set
      assertFalse(mutableIndex.getPresenceBitmap("key").contains(0));
      assertFalse(mutableIndex.getKeys().contains("key"));
    }
  }

  private static org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext buildMutableContext(
      FieldSpec fieldSpec) {
    return new org.apache.pinot.segment.spi.index.mutable.provider.MutableIndexContext(
        fieldSpec, -1, false, "testSegment", null, 1000, false, 100, 1000, 1, null);
  }

  @Test
  public void testInvalidMagicBytesThrows()
      throws Exception {
    // Build a minimal valid index
    String colName = "magic_test";
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(colName);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 10);
    OnHeapColumnarMapIndexCreator creator =
        new OnHeapColumnarMapIndexCreator(INDEX_DIR, colName, fieldSpec, config);
    creator.add(Map.of("k", 42));
    creator.seal();
    creator.close();

    // Corrupt magic bytes
    File indexFile = new File(INDEX_DIR, colName + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    try (RandomAccessFile raf = new RandomAccessFile(indexFile, "rw")) {
      raf.seek(0);
      raf.writeInt(0xDEADBEEF);
    }

    // Verify reader throws
    PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    assertThrows(IOException.class, () -> new ImmutableColumnarMapIndexReader(buf, null));
    buf.close();
  }

  @Test
  public void testConcurrentAddAndReadDoesNotThrow()
      throws Exception {
    ComplexFieldSpec fieldSpec = buildMapFieldSpec("concurrent_test");
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 100);
    MutableColumnarMapIndexImpl idx;
    try (MutableColumnarMapIndexImpl tmp = new MutableColumnarMapIndexImpl(buildMutableContext(fieldSpec), config)) {
      idx = tmp;
      // Note: MutableColumnarMapIndexImpl.close() is a no-op, so this is safe

      ExecutorService pool = Executors.newFixedThreadPool(4);
      AtomicBoolean failed = new AtomicBoolean(false);

      // 2 writer threads
      for (int t = 0; t < 2; t++) {
        final int thread = t;
        pool.submit(() -> {
          for (int i = 0; i < 200; i++) {
            try {
              idx.add(Map.of("k" + (thread * 200 + i), i), -1, thread * 200 + i);
            } catch (Exception e) {
              failed.set(true);
            }
          }
        });
      }

      // 2 reader threads
      for (int t = 0; t < 2; t++) {
        final int thread = t;
        pool.submit(() -> {
          for (int i = 0; i < 200; i++) {
            try {
              idx.getMap(thread * 200 + i);
              idx.getKeys();
            } catch (Exception e) {
              failed.set(true);
            }
          }
        });
      }

      pool.shutdown();
      pool.awaitTermination(15, TimeUnit.SECONDS);
      assertFalse(failed.get(), "Concurrent add/read threw an exception");
    }
  }

  @Test
  public void testMaxKeysRetainsExactlyMaxKeysKeys()
      throws Exception {
    String colName = "maxkeys_exact_test";
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(colName);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 2);
    OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(INDEX_DIR, colName, fieldSpec, config);

    // Add keys one at a time, in known order, across multiple documents
    // "alpha" first (doc 0), "beta" second (doc 1), "gamma" third (doc 2, should be dropped)
    creator.add(Map.of("alpha", 1));  // doc 0
    creator.add(Map.of("beta", 2));   // doc 1
    creator.add(Map.of("gamma", 3));  // doc 2 — gamma should be dropped (maxKeys=2)
    creator.seal();
    creator.close();

    File indexFile = new File(INDEX_DIR, colName + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buf, null);

    assertEquals(reader.getKeys().size(), 2, "Index must hold exactly maxKeys=2 keys");
    assertTrue(reader.getKeys().contains("alpha"), "alpha was first key, must be retained");
    assertTrue(reader.getKeys().contains("beta"), "beta was second key, must be retained");
    assertFalse(reader.getKeys().contains("gamma"), "gamma was third key and must be dropped");

    // Presence bitmaps must be correct
    assertTrue(reader.getPresenceBitmap("alpha").contains(0));
    assertTrue(reader.getPresenceBitmap("beta").contains(1));
    assertEquals(reader.getPresenceBitmap("gamma").getCardinality(), 0); // empty — key not indexed

    buf.close();
  }

  @Test
  public void testSealAfterManyAddsProducesValidIndex()
      throws Exception {
    String colName = "many_docs_test";
    Map<String, FieldSpec.DataType> keyTypes = Collections.singletonMap("count", FieldSpec.DataType.INT);
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(colName);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 10);
    OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(INDEX_DIR, colName, fieldSpec, config,
        keyTypes, FieldSpec.DataType.STRING);

    int numDocs = 500;
    for (int i = 0; i < numDocs; i++) {
      if (i % 3 == 0) {
        creator.add(Map.of("count", i));  // every 3rd doc has "count"
      } else {
        creator.add(Collections.emptyMap());  // absent
      }
    }
    creator.seal();
    creator.close();

    File indexFile = new File(INDEX_DIR, colName + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buf, null);

    // Exactly numDocs/3 (rounded up) docs have "count"
    int expectedCount = (numDocs + 2) / 3;  // ceil(500/3) = 167
    assertEquals(reader.getPresenceBitmap("count").getCardinality(), expectedCount);

    // Spot check: doc 0 has count=0, doc 3 has count=3, doc 1 is absent
    assertTrue(reader.getPresenceBitmap("count").contains(0));
    assertTrue(reader.getPresenceBitmap("count").contains(3));
    assertFalse(reader.getPresenceBitmap("count").contains(1));
    assertEquals(reader.getInt(0, "count"), 0);
    assertEquals(reader.getInt(3, "count"), 3);

    buf.close();
  }

  @Test
  public void testByteOrderRoundTripAllTypes()
      throws Exception {
    String colName = "byteorder_test";
    Map<String, FieldSpec.DataType> keyTypes = Map.of(
        "intKey", FieldSpec.DataType.INT,
        "longKey", FieldSpec.DataType.LONG,
        "floatKey", FieldSpec.DataType.FLOAT,
        "doubleKey", FieldSpec.DataType.DOUBLE
    );
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(colName);

    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 10);
    try (OnHeapColumnarMapIndexCreator creator =
        new OnHeapColumnarMapIndexCreator(INDEX_DIR, colName, fieldSpec, config, keyTypes, FieldSpec.DataType.STRING)) {
      creator.add(Map.of(
          "intKey", 0x12345678,
          "longKey", 0x123456789ABCDEF0L,
          "floatKey", 3.14f,
          "doubleKey", 2.718281828
      ));
      creator.seal();
    }

    File indexFile = new File(INDEX_DIR, colName + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buf, null);

    assertEquals(reader.getInt(0, "intKey"), 0x12345678);
    assertEquals(reader.getLong(0, "longKey"), 0x123456789ABCDEF0L);
    assertEquals(reader.getFloat(0, "floatKey"), 3.14f, 0.0001f);
    assertEquals(reader.getDouble(0, "doubleKey"), 2.718281828, 0.000000001);

    reader.close();
    buf.close();
  }

  // ---- Per-key inverted index tests ----

  @Test
  public void testPerKeyInvertedIndexSelectiveKeys()
      throws IOException {
    // enableInvertedIndexForAll=false + invertedIndexKeys=["color"] → only "color" gets inverted index
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("color", FieldSpec.DataType.STRING);
    keyTypes.put("size", FieldSpec.DataType.STRING);

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config =
        new ColumnarMapIndexConfig(true, null, false, Set.of("color"), 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("color", "red", "size", "small"),
        Map.of("color", "blue", "size", "large"),
        Map.of("color", "red", "size", "medium")
    };

    try (OnHeapColumnarMapIndexCreator creator =
        new OnHeapColumnarMapIndexCreator(INDEX_DIR, COLUMN_NAME, fieldSpec, config,
            keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {
      // color has inverted index
      ImmutableRoaringBitmap reds = reader.getDocsWithKeyValue("color", "red");
      assertNotNull(reds, "color should have inverted index");
      assertTrue(reds.contains(0));
      assertTrue(reds.contains(2));
      assertFalse(reds.contains(1));

      // size does NOT have inverted index
      ImmutableRoaringBitmap smalls = reader.getDocsWithKeyValue("size", "small");
      assertNull(smalls, "size should NOT have inverted index");
    }
  }

  @Test
  public void testPerKeyInvertedIndexAllOverridesSelectiveKeys()
      throws IOException {
    // enableInvertedIndexForAll=true + invertedIndexKeys=["color"] → ALL keys get inverted index (list ignored)
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("color", FieldSpec.DataType.STRING);
    keyTypes.put("size", FieldSpec.DataType.STRING);

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config =
        new ColumnarMapIndexConfig(true, null, true, Set.of("color"), 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("color", "red", "size", "small"),
        Map.of("color", "blue", "size", "large")
    };

    try (OnHeapColumnarMapIndexCreator creator =
        new OnHeapColumnarMapIndexCreator(INDEX_DIR, COLUMN_NAME, fieldSpec, config,
            keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {
      // Both keys should have inverted indexes
      assertNotNull(reader.getDocsWithKeyValue("color", "red"));
      assertNotNull(reader.getDocsWithKeyValue("size", "small"));
    }
  }

  @Test
  public void testPerKeyInvertedIndexNoKeysSpecified()
      throws IOException {
    // enableInvertedIndexForAll=false + no invertedIndexKeys → no inverted indexes (existing behavior)
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("color", FieldSpec.DataType.STRING);

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{Map.of("color", "red")};

    try (OnHeapColumnarMapIndexCreator creator =
        new OnHeapColumnarMapIndexCreator(INDEX_DIR, COLUMN_NAME, fieldSpec, config,
            keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {
      assertNull(reader.getDocsWithKeyValue("color", "red"), "No inverted index should exist");
    }
  }

  @Test
  public void testPerKeyInvertedIndexMutableSelectiveKeys()
      throws IOException {
    // Mutable index: enableInvertedIndexForAll=false + invertedIndexKeys=["brand"] → only brand gets inverted index
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("brand", FieldSpec.DataType.STRING);
    keyTypes.put("sku", FieldSpec.DataType.STRING);

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config =
        new ColumnarMapIndexConfig(true, null, false, Set.of("brand"), 100);

    try (MutableColumnarMapIndexImpl mutableIndex = new MutableColumnarMapIndexImpl(
        buildMutableContext(fieldSpec), config, keyTypes, FieldSpec.DataType.STRING)) {
      mutableIndex.add(Map.of("brand", "acme", "sku", "A1"), -1, 0);
      mutableIndex.add(Map.of("brand", "acme", "sku", "B2"), -1, 1);
      mutableIndex.add(Map.of("brand", "beta", "sku", "A1"), -1, 2);

      // brand has inverted index
      ImmutableRoaringBitmap acmeDocs = mutableIndex.getDocsWithKeyValue("brand", "acme");
      assertNotNull(acmeDocs, "brand should have inverted index");
      assertTrue(acmeDocs.contains(0));
      assertTrue(acmeDocs.contains(1));
      assertFalse(acmeDocs.contains(2));

      // sku does NOT have inverted index
      assertNull(mutableIndex.getDocsWithKeyValue("sku", "A1"),
          "sku should NOT have inverted index");
    }
  }

  @Test
  public void testConfigShouldEnableInvertedIndexForKey() {
    // Unit test for ColumnarMapIndexConfig.shouldEnableInvertedIndexForKey
    ColumnarMapIndexConfig allEnabled = new ColumnarMapIndexConfig(true, null, true, null, 100);
    assertTrue(allEnabled.shouldEnableInvertedIndexForKey("anyKey"));
    assertTrue(allEnabled.shouldEnableInvertedIndexForKey("anotherKey"));

    ColumnarMapIndexConfig selectiveEnabled =
        new ColumnarMapIndexConfig(true, null, false, Set.of("k1", "k2"), 100);
    assertTrue(selectiveEnabled.shouldEnableInvertedIndexForKey("k1"));
    assertTrue(selectiveEnabled.shouldEnableInvertedIndexForKey("k2"));
    assertFalse(selectiveEnabled.shouldEnableInvertedIndexForKey("k3"));

    ColumnarMapIndexConfig noneEnabled = new ColumnarMapIndexConfig(true, null, false, null, 100);
    assertFalse(noneEnabled.shouldEnableInvertedIndexForKey("anyKey"));

    // enableInvertedIndexForAll=true overrides invertedIndexKeys
    ColumnarMapIndexConfig allWithList =
        new ColumnarMapIndexConfig(true, null, true, Set.of("k1"), 100);
    assertTrue(allWithList.shouldEnableInvertedIndexForKey("k1"));
    assertTrue(allWithList.shouldEnableInvertedIndexForKey("k999"));
  }

  // ---- Dictionary-based GROUP BY tests ----

  @Test
  public void testColumnarMapKeyDictionary() {
    String[] values = {"apple", "banana", "cherry"};
    ColumnarMapKeyDictionary dict = new ColumnarMapKeyDictionary(FieldSpec.DataType.STRING, values);

    // length
    assertEquals(dict.length(), 3);

    // indexOf
    assertEquals(dict.indexOf("apple"), 0);
    assertEquals(dict.indexOf("banana"), 1);
    assertEquals(dict.indexOf("cherry"), 2);
    assertEquals(dict.indexOf("missing"), org.apache.pinot.segment.spi.index.reader.Dictionary.NULL_VALUE_INDEX);

    // get / getStringValue
    assertEquals(dict.get(0), "apple");
    assertEquals(dict.getStringValue(1), "banana");

    // isSorted
    assertTrue(dict.isSorted());

    // getValueType
    assertEquals(dict.getValueType(), FieldSpec.DataType.STRING);

    // getMinVal / getMaxVal
    assertEquals(dict.getMinVal(), "apple");
    assertEquals(dict.getMaxVal(), "cherry");

    // insertionIndexOf
    assertEquals(dict.insertionIndexOf("banana"), 1); // exact match
    assertTrue(dict.insertionIndexOf("blueberry") < 0); // not found, returns negative insertion point
  }

  @Test
  public void testColumnarMapKeyDictionaryIntType() {
    String[] values = {"10", "20", "30"};
    ColumnarMapKeyDictionary dict = new ColumnarMapKeyDictionary(FieldSpec.DataType.INT, values);

    assertEquals(dict.getIntValue(0), 10);
    assertEquals(dict.getLongValue(1), 20L);
    assertEquals(dict.getFloatValue(2), 30.0f, 0.001f);
    assertEquals(dict.getDoubleValue(0), 10.0, 0.001);
    assertEquals(dict.getMinVal(), 10);
    assertEquals(dict.getMaxVal(), 30);

    // indexOf with typed values
    assertEquals(dict.indexOf("20"), 1);
  }

  @Test
  public void testImmutableDistinctValues()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("color", FieldSpec.DataType.STRING);
    keyTypes.put("size", FieldSpec.DataType.INT);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("color", "red", "size", 10),
        Map.of("color", "blue", "size", 20),
        Map.of("color", "red", "size", 30),
        Map.of("color", "green", "size", 10)
    };

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    try (OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(
        INDEX_DIR, COLUMN_NAME, fieldSpec, config, keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      // hasInvertedIndex
      assertTrue(reader.hasInvertedIndex("color"));
      assertTrue(reader.hasInvertedIndex("size"));

      // getDistinctValuesForKey - color (sorted)
      String[] colorValues = reader.getDistinctValuesForKey("color");
      assertNotNull(colorValues);
      assertEquals(colorValues.length, 4);
      // Values should be sorted (includes default value "")
      assertEquals(colorValues[0], "");
      assertEquals(colorValues[1], "blue");
      assertEquals(colorValues[2], "green");
      assertEquals(colorValues[3], "red");

      // getDistinctValuesForKey - size (sorted, includes default "0")
      String[] sizeValues = reader.getDistinctValuesForKey("size");
      assertNotNull(sizeValues);
      assertEquals(sizeValues.length, 4);
      assertEquals(sizeValues[0], "0");
      assertEquals(sizeValues[1], "10");
      assertEquals(sizeValues[2], "20");
      assertEquals(sizeValues[3], "30");
    }
  }

  @Test
  public void testImmutableDistinctValuesNoInvertedIndex()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("name", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("name", "alice"),
        Map.of("name", "bob")
    };

    // No inverted index
    File indexFile = createIndex(keyTypes, docs);

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      assertFalse(reader.hasInvertedIndex("name"));
      assertNull(reader.getDistinctValuesForKey("name"));
    }
  }

  @Test
  public void testMutableDistinctValues()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("brand", FieldSpec.DataType.STRING);
    keyTypes.put("count", FieldSpec.DataType.INT);

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 100);

    try (MutableColumnarMapIndexImpl mutableIndex = new MutableColumnarMapIndexImpl(
        buildMutableContext(fieldSpec), config, keyTypes, FieldSpec.DataType.STRING)) {

      mutableIndex.add(Map.of("brand", "acme", "count", 5), -1, 0);
      mutableIndex.add(Map.of("brand", "beta", "count", 10), -1, 1);
      mutableIndex.add(Map.of("brand", "acme", "count", 5), -1, 2);

      // hasInvertedIndex
      assertTrue(mutableIndex.hasInvertedIndex("brand"));
      assertTrue(mutableIndex.hasInvertedIndex("count"));

      // getDistinctValuesForKey - brand (TreeMap → sorted)
      String[] brandValues = mutableIndex.getDistinctValuesForKey("brand");
      assertNotNull(brandValues);
      assertEquals(brandValues.length, 2);
      assertEquals(brandValues[0], "acme");
      assertEquals(brandValues[1], "beta");

      // getDistinctValuesForKey - count
      String[] countValues = mutableIndex.getDistinctValuesForKey("count");
      assertNotNull(countValues);
      assertEquals(countValues.length, 2);
      assertEquals(countValues[0], "10"); // TreeMap sorts lexicographically for strings
      assertEquals(countValues[1], "5");
    }
  }

  @Test
  public void testKeyDataSourceWithDictionary()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("status", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("status", "active"),
        Map.of("status", "inactive"),
        Map.of("status", "active")
    };

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    try (OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(
        INDEX_DIR, COLUMN_NAME, fieldSpec, config, keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      org.apache.pinot.segment.spi.datasource.DataSource ds =
          new ColumnarMapDataSource(buildColumnMetadata(fieldSpec, 3), reader).getKeyDataSource("status");
      assertNotNull(ds);

      // Dictionary contains actual values plus default: "", "active", "inactive" (sorted)
      org.apache.pinot.segment.spi.index.reader.Dictionary dict = ds.getDictionary();
      assertNotNull(dict, "Dictionary should be present when inverted index is available");
      assertEquals(dict.length(), 3); // "", "active", "inactive"
      assertTrue(dict.indexOf("active") >= 0);
      assertTrue(dict.indexOf("inactive") >= 0);

      // Forward index should report dictionary-encoded
      org.apache.pinot.segment.spi.index.reader.ForwardIndexReader<?> fwd = ds.getForwardIndex();
      assertTrue(fwd.isDictionaryEncoded());
    }
  }

  @Test
  public void testKeyDataSourceWithoutDictionary()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("name", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("name", "alice"),
        Map.of("name", "bob")
    };

    // Force raw encoding via noDictionaryKeys
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, false, null, Set.of("name"), 1000);
    File indexFile = createIndex(fieldSpec, config, docs);

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      org.apache.pinot.segment.spi.datasource.DataSource ds =
          new ColumnarMapDataSource(buildColumnMetadata(fieldSpec(keyTypes), 2), reader).getKeyDataSource("name");
      assertNotNull(ds);

      // No dictionary when noDictionaryKeys is set
      assertNull(ds.getDictionary(), "Dictionary should be null when noDictionaryKeys forces raw");

      // Forward index should NOT report dictionary-encoded
      org.apache.pinot.segment.spi.index.reader.ForwardIndexReader<?> fwd = ds.getForwardIndex();
      assertFalse(fwd.isDictionaryEncoded());
    }
  }

  @Test
  public void testDictIdForwardIndex()
      throws IOException {
    // Verify that bit-packed dictIds are written and readable at segment level
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("color", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("color", "red"),     // doc 0
        Map.of("color", "blue"),    // doc 1
        Map.of("color", "red"),     // doc 2
        new HashMap<>(),            // doc 3 — absent, gets default ""
        Map.of("color", "green")    // doc 4
    };

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    try (OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(
        INDEX_DIR, COLUMN_NAME, fieldSpec, config, keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      // Verify dictId reader is available
      FixedBitIntReaderWriter dictIdReader = reader.getDictIdReader("color");
      assertNotNull(dictIdReader, "dictId reader should be available for key with inverted index");

      // Verify key dictionary is available
      ColumnarMapKeyDictionary dict = reader.getKeyDictionary("color");
      assertNotNull(dict, "Key dictionary should be available");

      // Dictionary contains actual values plus default value: "", "blue", "green", "red" (sorted)
      assertEquals(dict.length(), 4);
      assertEquals(dict.get(0), "");
      assertEquals(dict.get(1), "blue");
      assertEquals(dict.get(2), "green");
      assertEquals(dict.get(3), "red");

      // DictId forward index is sparse: 4 entries for docs 0,1,2,4 (doc 3 absent)
      int redId = dict.indexOf("red");
      int blueId = dict.indexOf("blue");
      int greenId = dict.indexOf("green");

      assertEquals(dictIdReader.readInt(0), redId);     // ordinal 0 (doc 0) = red
      assertEquals(dictIdReader.readInt(1), blueId);    // ordinal 1 (doc 1) = blue
      assertEquals(dictIdReader.readInt(2), redId);     // ordinal 2 (doc 2) = red
      assertEquals(dictIdReader.readInt(3), greenId);   // ordinal 3 (doc 4) = green
    }
  }

  @Test
  public void testReadDictIdsFastPath()
      throws IOException {
    // Verify readDictIds uses bit-packed fast path and returns correct values
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("status", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("status", "active"),     // doc 0
        Map.of("status", "inactive"),   // doc 1
        Map.of("status", "active"),     // doc 2
        new HashMap<>(),                // doc 3 — absent
        Map.of("status", "pending")     // doc 4
    };

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    try (OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(
        INDEX_DIR, COLUMN_NAME, fieldSpec, config, keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      // Build DataSource and get per-key DataSource
      org.apache.pinot.segment.spi.datasource.DataSource ds =
          new ColumnarMapDataSource(buildColumnMetadata(fieldSpec, 5), reader).getKeyDataSource("status");
      assertNotNull(ds);

      org.apache.pinot.segment.spi.index.reader.ForwardIndexReader<?> fwd = ds.getForwardIndex();
      assertTrue(fwd.isDictionaryEncoded());

      org.apache.pinot.segment.spi.index.reader.Dictionary dict = ds.getDictionary();
      assertNotNull(dict);

      // Read dictIds including absent doc 3
      int[] docIds = {0, 1, 2, 3, 4};
      int[] dictIdBuffer = new int[5];
      fwd.readDictIds(docIds, 5, dictIdBuffer, null);

      // Verify via dictionary lookup
      assertEquals(dict.getStringValue(dictIdBuffer[0]), "active");
      assertEquals(dict.getStringValue(dictIdBuffer[1]), "inactive");
      assertEquals(dict.getStringValue(dictIdBuffer[2]), "active");
      // Absent doc gets default value's dictId (not NULL_VALUE_INDEX)
      int defaultDictId = dict.indexOf(ColumnarMapKeyDictionary.getDefaultValueString(FieldSpec.DataType.STRING));
      assertTrue(defaultDictId >= 0, "Default value should be in dictionary");
      assertEquals(dictIdBuffer[3], defaultDictId, "Absent doc should get default value dictId");
      assertEquals(dict.getStringValue(dictIdBuffer[4]), "pending");

      // Verify same dictIds for same values
      assertEquals(dictIdBuffer[0], dictIdBuffer[2], "Same value 'active' should have same dictId");
    }
  }

  @Test
  public void testReadDictIdsWithGaps()
      throws IOException {
    // Verify co-iterator readDictIds works correctly with non-sequential docIds (filtered GROUP BY)
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("color", FieldSpec.DataType.STRING);

    // 20 docs, key present in ~60%: docs 0,1,3,5,6,8,10,12,14,16,17,19
    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[20];
    String[] colors = {"red", "blue", "green", "red", "blue"};
    int[] presentDocs = {0, 1, 3, 5, 6, 8, 10, 12, 14, 16, 17, 19};
    Set<Integer> presentSet = new HashSet<>();
    for (int d : presentDocs) {
      presentSet.add(d);
    }
    int colorIdx = 0;
    for (int i = 0; i < 20; i++) {
      if (presentSet.contains(i)) {
        docs[i] = Map.of("color", colors[colorIdx % colors.length]);
        colorIdx++;
      } else {
        docs[i] = new HashMap<>();
      }
    }

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 1000);
    File indexFile =
        new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    try (OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(
        INDEX_DIR, COLUMN_NAME, fieldSpec, config, keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      org.apache.pinot.segment.spi.datasource.DataSource ds =
          new ColumnarMapDataSource(buildColumnMetadata(fieldSpec, 20), reader).getKeyDataSource("color");
      assertNotNull(ds);

      org.apache.pinot.segment.spi.index.reader.ForwardIndexReader<?> fwd = ds.getForwardIndex();
      assertTrue(fwd.isDictionaryEncoded());
      org.apache.pinot.segment.spi.index.reader.Dictionary dict = ds.getDictionary();
      assertNotNull(dict);

      // Case 1: sparse docIds with gaps (simulates filtered GROUP BY)
      int[] sparseDocIds = {0, 3, 7, 15, 19};
      int[] dictIdBuffer = new int[5];
      fwd.readDictIds(sparseDocIds, 5, dictIdBuffer, null);

      // Absent docs get default value's dictId
      int defaultDictId = dict.indexOf(ColumnarMapKeyDictionary.getDefaultValueString(FieldSpec.DataType.STRING));
      assertTrue(defaultDictId >= 0, "Default value should be in dictionary");

      assertEquals(dict.getStringValue(dictIdBuffer[0]), "red");    // doc 0 present
      assertEquals(dict.getStringValue(dictIdBuffer[1]), "green");  // doc 3 present
      assertEquals(dictIdBuffer[2], defaultDictId);                 // doc 7 absent
      assertEquals(dictIdBuffer[3], defaultDictId);                 // doc 15 absent
      assertEquals(dict.getStringValue(dictIdBuffer[4]), "blue");   // doc 19 present

      // Case 2: all docIds absent
      int[] absentDocIds = {2, 4, 7, 9, 11};
      int[] absentBuffer = new int[5];
      fwd.readDictIds(absentDocIds, 5, absentBuffer, null);
      for (int i = 0; i < 5; i++) {
        assertEquals(absentBuffer[i], defaultDictId,
            "Doc " + absentDocIds[i] + " should be absent");
      }

      // Case 3: all docIds present
      int[] allPresentDocIds = {0, 1, 3, 5, 6};
      int[] presentBuffer = new int[5];
      fwd.readDictIds(allPresentDocIds, 5, presentBuffer, null);
      for (int i = 0; i < 5; i++) {
        assertNotEquals(presentBuffer[i], Dictionary.NULL_VALUE_INDEX,
            "Doc " + allPresentDocIds[i] + " should be present");
      }

      // Case 4: single doc
      int[] singleDoc = {10};
      int[] singleBuffer = new int[1];
      fwd.readDictIds(singleDoc, 1, singleBuffer, null);
      assertNotEquals(singleBuffer[0], Dictionary.NULL_VALUE_INDEX);

      // Case 5: late docIds (simulates blocks from end of segment)
      int[] lateDocIds = {16, 17, 19};
      int[] lateBuffer = new int[3];
      fwd.readDictIds(lateDocIds, 3, lateBuffer, null);
      assertNotEquals(lateBuffer[0], Dictionary.NULL_VALUE_INDEX); // doc 16 present
      assertNotEquals(lateBuffer[1], Dictionary.NULL_VALUE_INDEX); // doc 17 present
      assertNotEquals(lateBuffer[2], Dictionary.NULL_VALUE_INDEX); // doc 19 present
    }
  }

  @Test
  public void testDictIdForwardIndexAvailableByDefault()
      throws IOException {
    // Dictionary encoding is now the default for all keys (even without inverted index)
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("name", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("name", "alice"),
        Map.of("name", "bob")
    };

    File indexFile = createIndex(keyTypes, docs);

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {
      assertNotNull(reader.getDictIdReader("name"), "DictId reader should be available by default");
      assertNotNull(reader.getKeyDictionary("name"), "Key dictionary should be available by default");

      assertEquals(reader.getString(0, "name"), "alice");
      assertEquals(reader.getString(1, "name"), "bob");
    }
  }

  @Test
  public void testBytesDictionaryRoundTrip()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("data", FieldSpec.DataType.BYTES);

    byte[] val1 = new byte[]{(byte) 0xFF, 0x00, 0x42};
    byte[] val2 = new byte[]{0x01, 0x02, 0x03};

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[]{
        Map.of("data", val1),
        Map.of("data", val2),
        Map.of("data", val1)
    };

    File indexFile = createIndex(keyTypes, docs);

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {
      assertEquals(reader.getBytes(0, "data"), val1);
      assertEquals(reader.getBytes(1, "data"), val2);
      assertEquals(reader.getBytes(2, "data"), val1);

      // Verify dictionary round-trip
      ColumnarMapKeyDictionary dict = reader.getKeyDictionary("data");
      assertNotNull(dict);
      for (int i = 0; i < dict.length(); i++) {
        byte[] bytesVal = dict.getBytesValue(i);
        String hexStr = dict.getStringValue(i);
        assertEquals(bytesVal, org.apache.pinot.spi.utils.BytesUtils.toBytes(hexStr),
            "getBytesValue must decode hex back to raw bytes");
      }
    }
  }

  private static ComplexFieldSpec fieldSpec(Map<String, FieldSpec.DataType> keyTypes) {
    return buildMapFieldSpec(COLUMN_NAME);
  }

  private static org.apache.pinot.segment.spi.ColumnMetadata buildColumnMetadata(
      FieldSpec fieldSpec, int numDocs) {
    return new org.apache.pinot.segment.spi.ColumnMetadata() {
      @Override
      public FieldSpec getFieldSpec() {
        return fieldSpec;
      }

      @Override
      public int getTotalDocs() {
        return numDocs;
      }

      @Override
      public int getTotalNumberOfEntries() {
        return numDocs;
      }

      @Override
      public int getCardinality() {
        return 0;
      }

      @Override
      public boolean isSorted() {
        return false;
      }

      @Override
      public int getBitsPerElement() {
        return 0;
      }

      @Override
      public int getColumnMaxLength() {
        return 0;
      }

      @Override
      public boolean hasDictionary() {
        return false;
      }

      @Override
      public org.apache.pinot.segment.spi.partition.PartitionFunction getPartitionFunction() {
        return null;
      }

      @Override
      public Set<Integer> getPartitions() {
        return null;
      }

      @Override
      public Comparable getMinValue() {
        return null;
      }

      @Override
      public Comparable getMaxValue() {
        return null;
      }

      @Override
      public boolean isMinMaxValueInvalid() {
        return true;
      }

      @Override
      public boolean isAutoGenerated() {
        return false;
      }

      @Override
      public int getMaxNumberOfMultiValues() {
        return 0;
      }

      @Override
      public java.util.Map<org.apache.pinot.segment.spi.index.IndexType<?, ?, ?>, Long> getIndexSizeMap() {
        return Collections.emptyMap();
      }
    };
  }

  // ---- GROUP BY readDictIds benchmark across key densities ----

  /**
   * Creates a segment with three MAP keys at different fill rates and benchmarks
   * readDictIds for each, comparing against a baseline FixedBitSVForwardIndexReader
   * that simulates how a flattened dimension column reads dictIds.
   *
   * This test validates:
   * 1. Correctness: every dictId resolves to the expected value at every density
   * 2. Performance profile: measures ns/doc for dense, medium, and sparse keys
   *    and compares against the equivalent flattened-column read path
   */
  @Test
  public void testReadDictIdsPerformanceByDensity()
      throws IOException {
    int numDocs = 100_000;
    double[] fillRates = {1.0, 0.6, 0.05};
    String[] keyNames = {"dense_key", "medium_key", "sparse_key"};
    String[] values = {"alpha", "beta", "gamma", "delta", "epsilon"};

    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    for (String k : keyNames) {
      keyTypes.put(k, FieldSpec.DataType.STRING);
    }

    // Build docs with controlled fill rates per key
    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[numDocs];
    // Track expected value per doc per key for correctness validation
    String[][] expectedValues = new String[keyNames.length][numDocs]; // null = absent
    java.util.Random rng = new java.util.Random(42);

    for (int d = 0; d < numDocs; d++) {
      Map<String, Object> doc = new HashMap<>();
      for (int k = 0; k < keyNames.length; k++) {
        if (rng.nextDouble() < fillRates[k]) {
          String val = values[rng.nextInt(values.length)];
          doc.put(keyNames[k], val);
          expectedValues[k][d] = val;
        }
      }
      docs[d] = doc;
    }

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 1000);
    File indexFile = new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    try (OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(
        INDEX_DIR, COLUMN_NAME, fieldSpec, config, keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      ColumnarMapDataSource mapDataSource = new ColumnarMapDataSource(buildColumnMetadata(fieldSpec, numDocs), reader);

      int blockSize = 10_000;
      int numBlocks = numDocs / blockSize;

      for (int k = 0; k < keyNames.length; k++) {
        String key = keyNames[k];
        org.apache.pinot.segment.spi.datasource.DataSource keyDs = mapDataSource.getKeyDataSource(key);
        assertNotNull(keyDs, "Key DataSource should exist for: " + key);

        org.apache.pinot.segment.spi.index.reader.ForwardIndexReader<?> fwd = keyDs.getForwardIndex();
        assertTrue(fwd.isDictionaryEncoded(), key + " should be dictionary encoded");
        org.apache.pinot.segment.spi.index.reader.Dictionary dict = keyDs.getDictionary();
        assertNotNull(dict, key + " dictionary should not be null");

        ImmutableRoaringBitmap presence = reader.getPresenceBitmap(key);
        long presentCount = presence.getLongCardinality();
        double actualFillRate = (double) presentCount / numDocs;

        // --- Correctness: read all docs in blocks and verify every value ---
        int correctCount = 0;
        int absentCorrectCount = 0;

        for (int b = 0; b < numBlocks; b++) {
          int start = b * blockSize;
          int[] docIds = new int[blockSize];
          for (int i = 0; i < blockSize; i++) {
            docIds[i] = start + i;
          }

          int[] dictIdBuffer = new int[blockSize];
          fwd.readDictIds(docIds, blockSize, dictIdBuffer, null);

          for (int i = 0; i < blockSize; i++) {
            int docId = start + i;
            String resolved = dict.getStringValue(dictIdBuffer[i]);

            if (expectedValues[k][docId] != null) {
              assertEquals(resolved, expectedValues[k][docId],
                  key + " doc " + docId + " expected " + expectedValues[k][docId] + " got " + resolved);
              correctCount++;
            } else {
              // Absent doc — should get default value (empty string for STRING type)
              assertEquals(resolved, "",
                  key + " doc " + docId + " absent but got non-default: " + resolved);
              absentCorrectCount++;
            }
          }
        }

        // --- Performance: time readDictIds over all blocks, 3 runs ---
        long[] timesNs = new long[3];
        for (int run = 0; run < 3; run++) {
          long startNs = System.nanoTime();
          for (int b = 0; b < numBlocks; b++) {
            int start = b * blockSize;
            int[] docIds = new int[blockSize];
            for (int i = 0; i < blockSize; i++) {
              docIds[i] = start + i;
            }
            int[] dictIdBuffer = new int[blockSize];
            fwd.readDictIds(docIds, blockSize, dictIdBuffer, null);
          }
          timesNs[run] = System.nanoTime() - startNs;
        }
        java.util.Arrays.sort(timesNs);
        long medianNs = timesNs[1];
        double nsPerDoc = (double) medianNs / numDocs;

        System.out.printf("[ColumnarMap] key=%-12s fill=%.0f%% (%6d/%d) correct=%d absent=%d "
                + "| median=%.1fms (%.0f ns/doc)%n",
            key, actualFillRate * 100, presentCount, numDocs,
            correctCount, absentCorrectCount, medianNs / 1e6, nsPerDoc);
      }

      // --- Baseline: simulate flattened column with FixedBitSVForwardIndexReader ---
      // Build a FixedBitIntReaderWriter with the same dictIds as the dense key, indexed by docId
      org.apache.pinot.segment.spi.datasource.DataSource denseDs = mapDataSource.getKeyDataSource("dense_key");
      org.apache.pinot.segment.spi.index.reader.Dictionary denseDict = denseDs.getDictionary();
      int numBits = org.apache.pinot.segment.local.io.util.PinotDataBitSet.getNumBitsPerValue(
          Math.max(denseDict.length() - 1, 0));

      // Write dictIds into a flat forward index buffer
      long bufferSize = ((long) numDocs * numBits + Byte.SIZE - 1) / Byte.SIZE;
      File flatFile = new File(INDEX_DIR, "flat_fwd.idx");
      try (PinotDataBuffer flatBuf = PinotDataBuffer.mapFile(flatFile, false, 0, bufferSize,
          java.nio.ByteOrder.BIG_ENDIAN, "flat-fwd");
          FixedBitIntReaderWriter flatWriter = new FixedBitIntReaderWriter(flatBuf, numDocs, numBits)) {

        // Read dictIds from columnar map to populate the flat forward index
        org.apache.pinot.segment.spi.index.reader.ForwardIndexReader<?> denseFwd = denseDs.getForwardIndex();
        for (int b = 0; b < numBlocks; b++) {
          int start = b * blockSize;
          int[] docIds = new int[blockSize];
          for (int i = 0; i < blockSize; i++) {
            docIds[i] = start + i;
          }
          int[] dictIdBuffer = new int[blockSize];
          denseFwd.readDictIds(docIds, blockSize, dictIdBuffer, null);
          for (int i = 0; i < blockSize; i++) {
            flatWriter.writeInt(start + i, dictIdBuffer[i]);
          }
        }

        // Now benchmark reading from the flat forward index (simulates FixedBitSVForwardIndexReader)
        long[] flatTimesNs = new long[3];
        for (int run = 0; run < 3; run++) {
          long startNs = System.nanoTime();
          for (int b = 0; b < numBlocks; b++) {
            int start = b * blockSize;
            int[] docIds = new int[blockSize];
            for (int i = 0; i < blockSize; i++) {
              docIds[i] = start + i;
            }
            int[] dictIdBuffer = new int[blockSize];
            for (int i = 0; i < blockSize; i++) {
              dictIdBuffer[i] = flatWriter.readInt(docIds[i]);
            }
          }
          flatTimesNs[run] = System.nanoTime() - startNs;
        }
        java.util.Arrays.sort(flatTimesNs);
        long flatMedianNs = flatTimesNs[1];
        double flatNsPerDoc = (double) flatMedianNs / numDocs;

        System.out.printf("[Flat FwdIdx] baseline                              "
                + "| median=%.1fms (%.0f ns/doc)%n", flatMedianNs / 1e6, flatNsPerDoc);
      }
    }
  }

  // ---- Micro-benchmark isolating each cost component in readDictIds ----

  /**
   * Isolates the overhead sources in ColumnarMapKeyForwardIndexReader.readDictIds():
   *
   * 1. "flat baseline" — FixedBitIntReaderWriter.readInt(docIds[i]) per doc (the target)
   * 2. "flat batch" — FixedBitIntReaderWriter.readInt(startIndex, length, buffer) (sequential batch)
   * 3. "bitmap iter only" — co-iterator walk without any readInt (cost of bitmap traversal alone)
   * 4. "rankLong only" — per-block rankLong() without iterator (cost of the seed operation)
   * 5. "iter + readInt(ordinal)" — full columnar map path (current readDictIds)
   * 6. "rank per doc" — rankLong() per doc instead of iterator (alternative approach)
   * 7. "dense shortcut" — skip bitmap entirely when fill=100%, read docId as ordinal
   */
  @Test
  public void testReadDictIdsMicroBenchmark()
      throws IOException {
    double[] fillRates = {1.0, 0.6, 0.05};
    for (double fillRate : fillRates) {
      runMicroBenchmarkForFillRate(fillRate);
    }
  }

  private void runMicroBenchmarkForFillRate(double fillRate)
      throws IOException {
    // Clean up any previous index files for this run
    FileUtils.cleanDirectory(INDEX_DIR);

    int numDocs = 500_000;
    int blockSize = 10_000;
    int numBlocks = numDocs / blockSize;
    int warmupRuns = 3;
    int measuredRuns = 5;
    String[] values = {"alpha", "beta", "gamma", "delta", "epsilon"};

    // Build segment with a single 100% fill key
    Map<String, FieldSpec.DataType> keyTypes = Map.of("test_key", FieldSpec.DataType.STRING);

    @SuppressWarnings("unchecked")
    Map<String, Object>[] docs = new Map[numDocs];
    java.util.Random rng = new java.util.Random(42);
    for (int d = 0; d < numDocs; d++) {
      Map<String, Object> doc = new HashMap<>();
      if (rng.nextDouble() < fillRate) {
        doc.put("test_key", values[rng.nextInt(values.length)]);
      }
      docs[d] = doc;
    }

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 1000);
    File indexFile = new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

    try (OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(
        INDEX_DIR, COLUMN_NAME, fieldSpec, config, keyTypes, FieldSpec.DataType.STRING)) {
      for (Map<String, Object> doc : docs) {
        creator.add(doc);
      }
      creator.seal();
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
        ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

      ColumnarMapDataSource mapDataSource = new ColumnarMapDataSource(buildColumnMetadata(fieldSpec, numDocs), reader);
      org.apache.pinot.segment.spi.datasource.DataSource keyDs = mapDataSource.getKeyDataSource("test_key");
      org.apache.pinot.segment.spi.index.reader.ForwardIndexReader<?> fwd = keyDs.getForwardIndex();
      org.apache.pinot.segment.spi.index.reader.Dictionary dict = keyDs.getDictionary();
      ImmutableRoaringBitmap presence = reader.getPresenceBitmap("test_key");

      int numBits = org.apache.pinot.segment.local.io.util.PinotDataBitSet.getNumBitsPerValue(
          Math.max(dict.length() - 1, 0));

      // Build flat forward index for comparison
      long flatBufSize = ((long) numDocs * numBits + Byte.SIZE - 1) / Byte.SIZE;
      File flatFile = new File(INDEX_DIR, "micro_flat.idx");
      PinotDataBuffer flatBuf = PinotDataBuffer.mapFile(flatFile, false, 0, flatBufSize,
          java.nio.ByteOrder.BIG_ENDIAN, "flat-micro");
      FixedBitIntReaderWriter flatFwd = new FixedBitIntReaderWriter(flatBuf, numDocs, numBits);

      // Get the dictIdReader from the columnar map reader for direct access
      FixedBitIntReaderWriter cmDictIdReader = reader.getDictIdReader("test_key");
      assertNotNull(cmDictIdReader, "dictIdReader should exist for test_key");

      // Populate flat with same dictIds
      for (int b = 0; b < numBlocks; b++) {
        int start = b * blockSize;
        int[] docIds = new int[blockSize];
        for (int i = 0; i < blockSize; i++) {
          docIds[i] = start + i;
        }
        int[] dictIdBuf = new int[blockSize];
        fwd.readDictIds(docIds, blockSize, dictIdBuf, null);
        for (int i = 0; i < blockSize; i++) {
          flatFwd.writeInt(start + i, dictIdBuf[i]);
        }
      }

      System.out.printf("%n=== readDictIds Micro-Benchmark (%,d docs, %,d blocks of %,d) ===%n",
          numDocs, numBlocks, blockSize);
      System.out.printf("Key fill rate: %.0f%%, dict cardinality: %d, bits/value: %d%n%n",
          (double) presence.getLongCardinality() / numDocs * 100, dict.length(), numBits);

      // --- Test 1: Flat baseline (random access by docId) ---
      {
        long[] times = benchmarkRuns(warmupRuns, measuredRuns, () -> {
          for (int b = 0; b < numBlocks; b++) {
            int start = b * blockSize;
            int[] docIds = new int[blockSize];
            for (int i = 0; i < blockSize; i++) {
              docIds[i] = start + i;
            }
            int[] dictIdBuf = new int[blockSize];
            for (int i = 0; i < blockSize; i++) {
              dictIdBuf[i] = flatFwd.readInt(docIds[i]);
            }
          }
        });
        printBenchResult("1. flat readInt(docId)", times, numDocs);
      }

      // --- Test 2: Flat batch (sequential, no docId array) ---
      {
        long[] times = benchmarkRuns(warmupRuns, measuredRuns, () -> {
          for (int b = 0; b < numBlocks; b++) {
            int start = b * blockSize;
            int[] dictIdBuf = new int[blockSize];
            flatFwd.readInt(start, blockSize, dictIdBuf);
          }
        });
        printBenchResult("2. flat batch readInt(start, len, buf)", times, numDocs);
      }

      // --- Test 3: Bitmap iterator only (no readInt) ---
      {
        long[] times = benchmarkRuns(warmupRuns, measuredRuns, () -> {
          for (int b = 0; b < numBlocks; b++) {
            int start = b * blockSize;
            int end = start + blockSize;
            org.roaringbitmap.PeekableIntIterator iter = presence.getIntIterator();
            iter.advanceIfNeeded(start);
            int ordinal = (start == 0) ? 0 : (int) presence.rankLong(start - 1);
            int[] docIds = new int[blockSize];
            for (int i = 0; i < blockSize; i++) {
              docIds[i] = start + i;
            }
            for (int i = 0; i < blockSize; i++) {
              int docId = docIds[i];
              while (iter.hasNext() && iter.peekNext() < docId) {
                iter.next();
                ordinal++;
              }
              if (iter.hasNext() && iter.peekNext() == docId) {
                // just consume ordinal, don't read anything
                int o = ordinal;
                iter.next();
                ordinal++;
              }
            }
          }
        });
        printBenchResult("3. bitmap co-iterator only (no readInt)", times, numDocs);
      }

      // --- Test 4: rankLong per block only ---
      {
        long[] times = benchmarkRuns(warmupRuns, measuredRuns, () -> {
          for (int b = 0; b < numBlocks; b++) {
            int start = b * blockSize;
            int ordinal = (start == 0) ? 0 : (int) presence.rankLong(start - 1);
          }
        });
        printBenchResult("4. rankLong() per block only", times, numDocs);
      }

      // --- Test 5: Full columnar map readDictIds (current implementation) ---
      {
        long[] times = benchmarkRuns(warmupRuns, measuredRuns, () -> {
          for (int b = 0; b < numBlocks; b++) {
            int start = b * blockSize;
            int[] docIds = new int[blockSize];
            for (int i = 0; i < blockSize; i++) {
              docIds[i] = start + i;
            }
            int[] dictIdBuf = new int[blockSize];
            fwd.readDictIds(docIds, blockSize, dictIdBuf, null);
          }
        });
        printBenchResult("5. columnar map readDictIds (current)", times, numDocs);
      }

      // --- Test 6: rankLong per doc (alternative: no iterator) ---
      {
        long[] times = benchmarkRuns(warmupRuns, measuredRuns, () -> {
          for (int b = 0; b < numBlocks; b++) {
            int start = b * blockSize;
            int[] docIds = new int[blockSize];
            for (int i = 0; i < blockSize; i++) {
              docIds[i] = start + i;
            }
            int[] dictIdBuf = new int[blockSize];
            for (int i = 0; i < blockSize; i++) {
              int docId = docIds[i];
              if (presence.contains(docId)) {
                int ordinal = (int) presence.rankLong(docId) - 1;
                dictIdBuf[i] = cmDictIdReader.readInt(ordinal);
              } else {
                dictIdBuf[i] = 0;
              }
            }
          }
        });
        printBenchResult("6. rank per doc (contains+rankLong+readInt)", times, numDocs);
      }

      // --- Test 7: Dense shortcut (ordinal == docId, skip bitmap) --- only valid at 100% fill
      if (presence.getLongCardinality() == numDocs) {
        long[] times = benchmarkRuns(warmupRuns, measuredRuns, () -> {
          for (int b = 0; b < numBlocks; b++) {
            int start = b * blockSize;
            int[] docIds = new int[blockSize];
            for (int i = 0; i < blockSize; i++) {
              docIds[i] = start + i;
            }
            int[] dictIdBuf = new int[blockSize];
            for (int i = 0; i < blockSize; i++) {
              dictIdBuf[i] = cmDictIdReader.readInt(docIds[i]);
            }
          }
        });
        printBenchResult("7. dense shortcut (ordinal=docId, no bitmap)", times, numDocs);
      } else {
        System.out.printf("  %-50s (skipped — fill < 100%%)%n", "7. dense shortcut (ordinal=docId, no bitmap)");
      }

      // --- Test 8: Iterator + batch readInt from ordinal start ---
      // For 100% fill, all docs in a block map to contiguous ordinals.
      // So we can: rankLong(firstDocId) → batch readInt(ordinalStart, blockSize, buffer)
      {
        long[] times = benchmarkRuns(warmupRuns, measuredRuns, () -> {
          for (int b = 0; b < numBlocks; b++) {
            int start = b * blockSize;
            int ordinalStart = (start == 0) ? 0 : (int) presence.rankLong(start - 1);
            // Check if all docs in block are present (block is fully dense)
            int ordinalEnd = (int) presence.rankLong(start + blockSize - 1);
            int[] dictIdBuf = new int[blockSize];
            if (ordinalEnd - ordinalStart == blockSize) {
              // Fully dense block: batch read
              cmDictIdReader.readInt(ordinalStart, blockSize, dictIdBuf);
            } else {
              // Sparse block: fall back to co-iterator
              org.roaringbitmap.PeekableIntIterator iter = presence.getIntIterator();
              iter.advanceIfNeeded(start);
              int ordinal = ordinalStart;
              int[] docIds = new int[blockSize];
              for (int i = 0; i < blockSize; i++) {
                docIds[i] = start + i;
              }
              for (int i = 0; i < blockSize; i++) {
                int docId = docIds[i];
                while (iter.hasNext() && iter.peekNext() < docId) {
                  iter.next();
                  ordinal++;
                }
                if (iter.hasNext() && iter.peekNext() == docId) {
                  dictIdBuf[i] = cmDictIdReader.readInt(ordinal);
                  iter.next();
                  ordinal++;
                } else {
                  dictIdBuf[i] = 0;
                }
              }
            }
          }
        });
        printBenchResult("8. per-block dense check + batch readInt", times, numDocs);
      }

      // --- Test 9: Measure expansion cost (ordinal→docId indexed) at load time ---
      {
        long[] times = benchmarkRuns(warmupRuns, measuredRuns, () -> {
          int numBitsLocal = org.apache.pinot.segment.local.io.util.PinotDataBitSet.getNumBitsPerValue(
              Math.max(dict.length() - 1, 0));
          // Allocate expanded array
          long expandedSize = ((long) numDocs * numBitsLocal + Byte.SIZE - 1) / Byte.SIZE;
          // Simulate: iterate presence bitmap, read ordinal dictIds, write to expanded positions
          // Use a simple int[] to avoid file I/O overhead in the measurement
          int[] expanded = new int[numDocs];
          java.util.Arrays.fill(expanded, 0); // default dictId
          org.roaringbitmap.PeekableIntIterator expIter = presence.getIntIterator();
          int ord = 0;
          while (expIter.hasNext()) {
            int docId = expIter.next();
            expanded[docId] = cmDictIdReader.readInt(ord);
            ord++;
          }
        });
        printBenchResult("9. expansion cost (build docId-indexed array)", times, numDocs);
      }

      flatFwd.close();
      flatBuf.close();
    }
  }

  private long[] benchmarkRuns(int warmup, int measured, Runnable task) {
    for (int i = 0; i < warmup; i++) {
      task.run();
    }
    long[] times = new long[measured];
    for (int i = 0; i < measured; i++) {
      long start = System.nanoTime();
      task.run();
      times[i] = System.nanoTime() - start;
    }
    java.util.Arrays.sort(times);
    return times;
  }

  private void printBenchResult(String label, long[] sortedTimesNs, int numDocs) {
    long median = sortedTimesNs[sortedTimesNs.length / 2];
    double nsPerDoc = (double) median / numDocs;
    System.out.printf("  %-50s median=%6.1fms  %5.0f ns/doc%n", label, median / 1e6, nsPerDoc);
  }

  // ---- Threshold sweep: compare expanded vs co-iterator at each fill rate ----

  @Test
  public void testExpansionThresholdSweep()
      throws IOException {
    double[] fillRates = {0.005, 0.01, 0.02, 0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.80, 0.95, 1.0};
    int numDocs = 500_000;
    int blockSize = 10_000;
    int numBlocks = numDocs / blockSize;
    int warmupRuns = 3;
    int measuredRuns = 5;
    String[] values = {"alpha", "beta", "gamma", "delta", "epsilon"};

    System.out.printf("%n=== Expansion Threshold Sweep (%,d docs, %,d-doc blocks) ===%n", numDocs, blockSize);
    System.out.printf("%-8s  %12s  %12s  %8s  %12s  %12s%n",
        "Fill%", "CoIter ns/d", "Expand ns/d", "Speedup", "Ordinal KB", "Expanded KB");
    System.out.printf("%-8s  %12s  %12s  %8s  %12s  %12s%n",
        "------", "-----------", "-----------", "-------", "----------", "-----------");

    for (double fillRate : fillRates) {
      FileUtils.cleanDirectory(INDEX_DIR);

      Map<String, FieldSpec.DataType> keyTypes = Map.of("k", FieldSpec.DataType.STRING);
      @SuppressWarnings("unchecked")
      Map<String, Object>[] docs = new Map[numDocs];
      java.util.Random rng = new java.util.Random(42);
      for (int d = 0; d < numDocs; d++) {
        Map<String, Object> doc = new HashMap<>();
        if (rng.nextDouble() < fillRate) {
          doc.put("k", values[rng.nextInt(values.length)]);
        }
        docs[d] = doc;
      }

      ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
      ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 1000);
      File indexFile = new File(INDEX_DIR, COLUMN_NAME + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);

      try (OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(
          INDEX_DIR, COLUMN_NAME, fieldSpec, config, keyTypes, FieldSpec.DataType.STRING)) {
        for (Map<String, Object> doc : docs) {
          creator.add(doc);
        }
        creator.seal();
      }

      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
          ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buffer, null)) {

        ImmutableRoaringBitmap presence = reader.getPresenceBitmap("k");
        ColumnarMapKeyDictionary dict = reader.getKeyDictionary("k");
        FixedBitIntReaderWriter sparseDictIdReader = reader.getDictIdReader("k");
        if (dict == null || sparseDictIdReader == null) {
          System.out.printf("%-8s  (no dictionary — skipped)%n", String.format("%.1f%%", fillRate * 100));
          continue;
        }

        int numBits = org.apache.pinot.segment.local.io.util.PinotDataBitSet.getNumBitsPerValue(
            Math.max(dict.length() - 1, 0));
        long presentCount = presence.getLongCardinality();

        // Memory sizes
        long ordinalBytes = ((long) presentCount * numBits + Byte.SIZE - 1) / Byte.SIZE;
        long expandedBytes = ((long) numDocs * numBits + Byte.SIZE - 1) / Byte.SIZE;

        // Build co-iterator reader (sparse, with bitmap)
        String defaultValueStr = ColumnarMapKeyDictionary.getDefaultValueString(FieldSpec.DataType.STRING);
        int defaultDictId = dict.indexOf(defaultValueStr);
        ColumnarMapKeyForwardIndexReader coIterReader =
            new ColumnarMapKeyForwardIndexReader(reader, "k", FieldSpec.DataType.STRING, dict,
                sparseDictIdReader, presence);

        // Build expanded reader (docId-indexed, no bitmap)
        PinotDataBuffer expandedBuf = PinotDataBuffer.allocateDirect(
            expandedBytes, java.nio.ByteOrder.BIG_ENDIAN, "sweep-expanded");
        FixedBitIntReaderWriter expandedWriter = new FixedBitIntReaderWriter(expandedBuf, numDocs, numBits);
        for (int d = 0; d < numDocs; d++) {
          expandedWriter.writeInt(d, defaultDictId);
        }
        org.roaringbitmap.PeekableIntIterator iter = presence.getIntIterator();
        int ordinal = 0;
        while (iter.hasNext()) {
          int docId = iter.next();
          expandedWriter.writeInt(docId, sparseDictIdReader.readInt(ordinal));
          ordinal++;
        }
        ColumnarMapKeyForwardIndexReader expandedReader =
            new ColumnarMapKeyForwardIndexReader(reader, "k", FieldSpec.DataType.STRING, dict,
                expandedWriter, null);

        // Benchmark co-iterator path
        long[] coIterTimes = benchmarkRuns(warmupRuns, measuredRuns, () -> {
          for (int b = 0; b < numBlocks; b++) {
            int start = b * blockSize;
            int[] docIds = new int[blockSize];
            for (int i = 0; i < blockSize; i++) {
              docIds[i] = start + i;
            }
            int[] dictIdBuf = new int[blockSize];
            coIterReader.readDictIds(docIds, blockSize, dictIdBuf, null);
          }
        });

        // Benchmark expanded path
        long[] expandedTimes = benchmarkRuns(warmupRuns, measuredRuns, () -> {
          for (int b = 0; b < numBlocks; b++) {
            int start = b * blockSize;
            int[] docIds = new int[blockSize];
            for (int i = 0; i < blockSize; i++) {
              docIds[i] = start + i;
            }
            int[] dictIdBuf = new int[blockSize];
            expandedReader.readDictIds(docIds, blockSize, dictIdBuf, null);
          }
        });

        long coIterMedian = coIterTimes[coIterTimes.length / 2];
        long expandedMedian = expandedTimes[expandedTimes.length / 2];
        double coIterNs = (double) coIterMedian / numDocs;
        double expandedNs = (double) expandedMedian / numDocs;
        double speedup = coIterNs / expandedNs;

        System.out.printf("%-8s  %9.0f      %9.0f      %6.1fx    %9.1f    %9.1f%n",
            String.format("%.1f%%", fillRate * 100),
            coIterNs, expandedNs, speedup,
            ordinalBytes / 1024.0, expandedBytes / 1024.0);

        expandedWriter.close();
        expandedBuf.close();
      }
    }
  }

  /// Verifies that {@link ColumnarMapDataSource#getKeyDataSource(String)} returns a valid
  /// DataSource for keys not yet ingested into a mutable (consuming) segment, rather than
  /// returning null and causing an NPE in {@code ItemTransformFunction.init()}.
  @Test
  public void testMutableKeyDataSourceForUnseenKey()
      throws IOException {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("status", FieldSpec.DataType.STRING);
    keyTypes.put("count", FieldSpec.DataType.INT);

    ComplexFieldSpec fieldSpec = buildMapFieldSpec(COLUMN_NAME);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 100);

    try (MutableColumnarMapIndexImpl mutableIndex = new MutableColumnarMapIndexImpl(
        buildMutableContext(fieldSpec), config, keyTypes, FieldSpec.DataType.STRING)) {

      // Only ingest docs with "status" — "count" is never seen
      mutableIndex.add(Map.of("status", "active"), -1, 0);
      mutableIndex.add(Map.of("status", "inactive"), -1, 1);

      ColumnarMapDataSource mapDS = new ColumnarMapDataSource(fieldSpec, 2, mutableIndex);

      // Key with data — should work as before
      org.apache.pinot.segment.spi.datasource.DataSource statusDS = mapDS.getKeyDataSource("status");
      assertNotNull(statusDS, "DataSource for ingested key should not be null");
      assertEquals(statusDS.getDataSourceMetadata().getDataType(), FieldSpec.DataType.STRING);

      // Key with explicit type but no docs ingested — must not return null
      org.apache.pinot.segment.spi.datasource.DataSource countDS = mapDS.getKeyDataSource("count");
      assertNotNull(countDS, "DataSource for unseen key with explicit type should not be null");
      assertEquals(countDS.getDataSourceMetadata().getDataType(), FieldSpec.DataType.INT);

      // Completely unknown key — gets default type, must not return null
      org.apache.pinot.segment.spi.datasource.DataSource unknownDS = mapDS.getKeyDataSource("never_heard_of");
      assertNotNull(unknownDS, "DataSource for unknown key should not be null (uses default type)");
      assertEquals(unknownDS.getDataSourceMetadata().getDataType(), FieldSpec.DataType.STRING);

      // Forward index should return default values for all docs on unseen keys
      org.apache.pinot.segment.spi.index.reader.ForwardIndexReader<?> fwd = countDS.getForwardIndex();
      assertNotNull(fwd);
      assertEquals(fwd.getString(0, null), "");
      assertEquals(fwd.getString(1, null), "");
    }
  }
}
