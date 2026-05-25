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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class MutableColumnarMapIndexTest {
  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeMethod
  public void setUp() {
    _memoryManager = new DirectMemoryManager("MutableColumnarMapIndexTest");
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _memoryManager.close();
  }

  @Test
  public void testBasicIndexing()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setValueFieldSpecs(Map.of(
        "country", new DimensionFieldSpec("country", DataType.STRING, true),
        "clicks", new DimensionFieldSpec("clicks", DataType.LONG, true)));
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    MapIndexConfig config = new MapIndexConfig(true, false, null, null, 100, null, 0.5);

    try (MutableColumnarMapIndex index = new MutableColumnarMapIndex("metrics", fieldSpec, config,
        _memoryManager, 1000)) {
      index.index(0, Map.of("country", "US", "clicks", 100L));
      index.index(1, Map.of("country", "UK"));
      index.index(2, Map.of("clicks", 300L));

      assertEquals(index.getKeys().size(), 2);
      assertNotNull(index.getKeyColumn("country"));
      assertNotNull(index.getKeyColumn("clicks"));

      MutableColumnarMapIndex.MutableKeyColumn countryCol = index.getKeyColumn("country");
      assertTrue(countryCol.getPresenceBitmap().contains(0));
      assertTrue(countryCol.getPresenceBitmap().contains(1));
      assertFalse(countryCol.getPresenceBitmap().contains(2));
      assertEquals(countryCol.getValue(0), "US");
      assertEquals(countryCol.getValue(1), "UK");

      MutableColumnarMapIndex.MutableKeyColumn clicksCol = index.getKeyColumn("clicks");
      assertTrue(clicksCol.getPresenceBitmap().contains(0));
      assertFalse(clicksCol.getPresenceBitmap().contains(1));
      assertTrue(clicksCol.getPresenceBitmap().contains(2));
      assertEquals(clicksCol.getValue(0), 100L);
      assertEquals(clicksCol.getValue(2), 300L);

      // Verify dictionary is populated
      assertNotNull(countryCol.getDictionary());
      assertEquals(countryCol.getDictionary().length(), 2);
      assertTrue(countryCol.getDictionary().indexOf("US") >= 0);
      assertTrue(countryCol.getDictionary().indexOf("UK") >= 0);

      // Verify inverted index: "US" maps to doc 0
      int usId = countryCol.getDictionary().indexOf("US");
      assertTrue(countryCol.getInvertedIndex().getDocIds(usId).contains(0));
      assertFalse(countryCol.getInvertedIndex().getDocIds(usId).contains(1));

      // Verify inverted index: "UK" maps to doc 1
      int ukId = countryCol.getDictionary().indexOf("UK");
      assertTrue(countryCol.getInvertedIndex().getDocIds(ukId).contains(1));
      assertFalse(countryCol.getInvertedIndex().getDocIds(ukId).contains(0));

      // Verify clicks key: dictionary and inverted index
      assertNotNull(clicksCol.getDictionary());
      assertEquals(clicksCol.getDictionary().length(), 2);
    }
  }

  @Test
  public void testMaxKeysEnforced()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    MapIndexConfig config = new MapIndexConfig(true, false, null, null, 2, null, 0.5);

    try (MutableColumnarMapIndex index = new MutableColumnarMapIndex("metrics", fieldSpec, config,
        _memoryManager, 1000)) {
      index.index(0, Map.of("k1", "a", "k2", "b"));
      index.index(1, Map.of("k3", "c"));

      assertEquals(index.getKeys().size(), 2);
      assertNull(index.getKeyColumn("k3"));
    }
  }

  @Test
  public void testNullAndEmptyMaps()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    MapIndexConfig config = new MapIndexConfig(true, false, null, null, 100, null, 0.5);

    try (MutableColumnarMapIndex index = new MutableColumnarMapIndex("metrics", fieldSpec, config,
        _memoryManager, 1000)) {
      index.index(0, Map.of("country", "US"));
      index.index(1, null);
      index.index(2, Map.of());
      index.index(3, Map.of("country", "UK"));

      assertEquals(index.getKeys().size(), 1);
      MutableColumnarMapIndex.MutableKeyColumn countryCol = index.getKeyColumn("country");
      assertEquals(countryCol.getPresenceBitmap().getCardinality(), 2);
      assertTrue(countryCol.getPresenceBitmap().contains(0));
      assertTrue(countryCol.getPresenceBitmap().contains(3));
    }
  }

  @Test
  public void testCoercion()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setValueFieldSpecs(Map.of(
        "clicks", new DimensionFieldSpec("clicks", DataType.LONG, true)));
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    MapIndexConfig config = new MapIndexConfig(true, false, null, null, 100, null, 0.5);

    try (MutableColumnarMapIndex index = new MutableColumnarMapIndex("metrics", fieldSpec, config,
        _memoryManager, 1000)) {
      // Integer value coerced to Long
      index.index(0, Map.of("clicks", 42));
      MutableColumnarMapIndex.MutableKeyColumn clicksCol = index.getKeyColumn("clicks");
      assertEquals(clicksCol.getValue(0), 42L);

      // String value coerced to Long
      index.index(1, Map.of("clicks", "100"));
      assertEquals(clicksCol.getValue(1), 100L);
    }
  }

  @Test
  public void testNoBackfillNeeded()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    MapIndexConfig config = new MapIndexConfig(true, false, null, null, 100, null, 0.5);

    try (MutableColumnarMapIndex index = new MutableColumnarMapIndex("metrics", fieldSpec, config,
        _memoryManager, 1000)) {
      // Index docs 0-9 with only "country"
      for (int i = 0; i < 10; i++) {
        index.index(i, Map.of("country", "US"));
      }
      // Doc 10 introduces "clicks" — no backfill for docs 0-9
      index.index(10, Map.of("country", "UK", "clicks", 500L));

      MutableColumnarMapIndex.MutableKeyColumn clicksCol = index.getKeyColumn("clicks");
      assertEquals(clicksCol.getPresenceBitmap().getCardinality(), 1);
      assertTrue(clicksCol.getPresenceBitmap().contains(10));
      // Docs 0-9 are not in presence bitmap — query layer treats them as null
    }
  }

  @Test
  public void testConcurrentReadDuringWrite()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setValueFieldSpecs(Map.of(
        "country", new DimensionFieldSpec("country", DataType.STRING, true),
        "clicks", new DimensionFieldSpec("clicks", DataType.LONG, true)));
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    MapIndexConfig config = new MapIndexConfig(true, false, null, null, 1000, null, 0.1);

    try (MutableColumnarMapIndex index = new MutableColumnarMapIndex("metrics", fieldSpec, config,
        _memoryManager, 10000)) {
      int numDocs = 1000;
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch doneLatch = new CountDownLatch(1);

      Thread writerThread = new Thread(() -> {
        try {
          startLatch.await();
        } catch (InterruptedException e) {
          return;
        }
        for (int i = 0; i < numDocs; i++) {
          Map<String, Object> row = Map.of("country", "US_" + (i % 10), "clicks", (long) i);
          index.index(i, row);
        }
        doneLatch.countDown();
      });

      writerThread.start();
      startLatch.countDown();

      List<Exception> readErrors = new ArrayList<>();
      while (doneLatch.getCount() > 0) {
        try {
          MutableColumnarMapIndex.MutableKeyColumn countryCol = index.getKeyColumn("country");
          if (countryCol != null) {
            int card = countryCol.getPresenceBitmap().getCardinality();
            assertTrue(card >= 0);
            if (card > 0) {
              Object val = countryCol.getValue(0);
              // Value may be null during concurrent dictionary resize
            }
          }
        } catch (Exception e) {
          readErrors.add(e);
        }
      }

      writerThread.join(5000);
      assertTrue(readErrors.isEmpty(), "Read errors during concurrent access: " + readErrors);

      MutableColumnarMapIndex.MutableKeyColumn countryCol = index.getKeyColumn("country");
      assertNotNull(countryCol);
      assertEquals(countryCol.getPresenceBitmap().getCardinality(), numDocs);
    }
  }

  @Test
  public void testImplementsColumnarMapIndexReader() {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setValueFieldSpecs(Map.of(
        "k1", new DimensionFieldSpec("k1", DataType.LONG, true)));
    MapIndexConfig config =
        new MapIndexConfig(true, false, null, null, 100, null, 0.0);
    MutableColumnarMapIndex index = new MutableColumnarMapIndex("metrics", fieldSpec, config,
        _memoryManager, 1000);
    index.index(0, Map.of("k1", 100L));
    index.index(1, Map.of("k1", 200L));

    // SPI assertions
    org.apache.pinot.segment.spi.index.reader.ColumnarMapIndexReader reader = index;
    assertEquals(reader.getKeys().size(), 1);
    assertEquals(reader.getValueType("k1"), DataType.LONG);
    assertEquals(reader.getNumDocsWithKey("k1"), 2);
    assertTrue(reader.getPresenceBitmap("k1").contains(0));
    assertTrue(reader.getPresenceBitmap("k1").contains(1));
    assertEquals(reader.getMap(0).get("k1"), 100L);
    assertEquals(reader.getMap(1).get("k1"), 200L);
    // Unindexed-key contracts
    assertEquals(reader.getNumDocsWithKey("missing"), 0);
    assertTrue(reader.getPresenceBitmap("missing").isEmpty());
    assertNull(reader.getValueType("missing"));
  }

  @Test
  public void testDictionaryRoundTrip()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setValueFieldSpecs(Map.of(
        "status", new DimensionFieldSpec("status", DataType.STRING, true)));
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    MapIndexConfig config = new MapIndexConfig(true, false, null, null, 100, null, 0.5);

    try (MutableColumnarMapIndex index = new MutableColumnarMapIndex("metrics", fieldSpec, config,
        _memoryManager, 1000)) {
      index.index(0, Map.of("status", "active"));
      index.index(1, Map.of("status", "inactive"));
      index.index(2, Map.of("status", "active"));

      MutableColumnarMapIndex.MutableKeyColumn statusCol = index.getKeyColumn("status");

      assertEquals(statusCol.getDictionary().length(), 2);

      assertEquals(statusCol.getValue(0), "active");
      assertEquals(statusCol.getValue(1), "inactive");
      assertEquals(statusCol.getValue(2), "active");

      int activeId = statusCol.getDictionary().indexOf("active");
      assertTrue(statusCol.getInvertedIndex().getDocIds(activeId).contains(0));
      assertTrue(statusCol.getInvertedIndex().getDocIds(activeId).contains(2));
      assertFalse(statusCol.getInvertedIndex().getDocIds(activeId).contains(1));
    }
  }

  @Test
  public void testKeysWithDoubleUnderscores()
      throws Exception {
    ComplexFieldSpec fieldSpec = new ComplexFieldSpec("metrics", DataType.MAP, true);
    fieldSpec.setDefaultValueFieldSpec(new DimensionFieldSpec("default", DataType.STRING, true));

    MapIndexConfig config = new MapIndexConfig(true, false, null, null, 100, null, 0.5);

    try (MutableColumnarMapIndex index = new MutableColumnarMapIndex("metrics", fieldSpec, config,
        _memoryManager, 1000)) {
      index.index(0, Map.of("user__name", "alice", "click__count__total", "99"));
      index.index(1, Map.of("user__name", "bob", "simple", "val"));

      assertEquals(index.getKeys().size(), 3);
      assertNotNull(index.getKeyColumn("user__name"));
      assertNotNull(index.getKeyColumn("click__count__total"));
      assertNotNull(index.getKeyColumn("simple"));

      MutableColumnarMapIndex.MutableKeyColumn userCol = index.getKeyColumn("user__name");
      assertEquals(userCol.getValue(0), "alice");
      assertEquals(userCol.getValue(1), "bob");

      MutableColumnarMapIndex.MutableKeyColumn clickCol = index.getKeyColumn("click__count__total");
      assertEquals(clickCol.getValue(0), "99");
      assertEquals(clickCol.getPresenceBitmap().getCardinality(), 1);
    }
  }
}
