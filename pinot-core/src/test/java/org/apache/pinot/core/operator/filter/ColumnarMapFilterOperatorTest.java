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
package org.apache.pinot.core.operator.filter;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.IsNotNullPredicate;
import org.apache.pinot.common.request.context.predicate.IsNullPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.segment.local.segment.index.columnarmap.ImmutableColumnarMapIndexReader;
import org.apache.pinot.segment.local.segment.index.columnarmap.OnHeapColumnarMapIndexCreator;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.config.table.ColumnarMapIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link ColumnarMapFilterOperator} verifying NOT_EQ and NOT_IN absence-exclusion semantics.
 * Docs where the queried key is absent must NOT appear in NOT_EQ / NOT_IN results (SQL NULL semantics).
 */
public class ColumnarMapFilterOperatorTest {

  private File _tmpDir;

  @BeforeClass
  public void setUp()
      throws Exception {
    _tmpDir = new File(System.getProperty("java.io.tmpdir"),
        "ColumnarMapFilterOperatorTest_" + System.currentTimeMillis());
    _tmpDir.mkdirs();
  }

  @AfterClass
  public void tearDown() {
    deleteDir(_tmpDir);
  }

  private static void deleteDir(File f) {
    if (f == null) {
      return;
    }
    if (f.isDirectory()) {
      File[] children = f.listFiles();
      if (children != null) {
        for (File c : children) {
          deleteDir(c);
        }
      }
    }
    f.delete();
  }

  private static ComplexFieldSpec buildMapFieldSpec(String columnName) {
    Map<String, FieldSpec> childFieldSpecs = Map.of(
        "key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
        "value", new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true)
    );
    return new ComplexFieldSpec(columnName, FieldSpec.DataType.MAP, true, childFieldSpecs);
  }

  /**
   * Builds an ImmutableColumnarMapIndexReader for a column with 4 docs:
   *   doc 0: color = "red"
   *   doc 1: color = "blue"
   *   doc 2: (no color key — absent)
   *   doc 3: color = "green"
   */
  private ImmutableColumnarMapIndexReader buildColorIndex(String colName)
      throws Exception {
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(colName);
    // enableInvertedIndexForAll=true so that getDocsWithKeyValue() works
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 10);

    OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(_tmpDir, colName, fieldSpec, config);
    Map<String, Object> doc = new HashMap<>();
    doc.put("color", "red");
    creator.add(doc);    // doc 0: color=red
    doc.clear();
    doc.put("color", "blue");
    creator.add(doc);    // doc 1: color=blue
    creator.add(Collections.emptyMap());  // doc 2: absent
    doc.clear();
    doc.put("color", "green");
    creator.add(doc);    // doc 3: color=green
    creator.seal();
    creator.close();

    File indexFile = new File(_tmpDir, colName + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    return new ImmutableColumnarMapIndexReader(buf, null);
  }

  /**
   * NOT_EQ: docs where key is absent must NOT appear in results.
   * color != "red" should return only docs where color IS present and != "red" → {1, 3}
   */
  @Test
  public void testNotEqExcludesDocsWhereKeyIsAbsent()
      throws Exception {
    ImmutableColumnarMapIndexReader reader = buildColorIndex("color_not_eq");
    NotEqPredicate predicate = new NotEqPredicate(ExpressionContext.forIdentifier("color"), "red");
    ColumnarMapFilterOperator op = new ColumnarMapFilterOperator(reader, predicate, "color", 4);

    ImmutableRoaringBitmap result = op.getBitmaps().reduce();
    assertEquals(result.getCardinality(), 2, "Expected exactly 2 matching docs: {1, 3}");
    assertFalse(result.contains(0), "doc 0 (color=red) must be excluded");
    assertFalse(result.contains(2), "doc 2 (color absent) must be excluded");
    assertEquals(result.toArray(), new int[]{1, 3});
  }

  /**
   * NOT_IN: docs where key is absent must NOT appear in results.
   * color NOT IN ("red") should return only docs where color IS present and not in {"red"} → {1, 3}
   */
  @Test
  public void testNotInExcludesDocsWhereKeyIsAbsent()
      throws Exception {
    ImmutableColumnarMapIndexReader reader = buildColorIndex("color_not_in");
    NotInPredicate predicate =
        new NotInPredicate(ExpressionContext.forIdentifier("color"), Arrays.asList("red"));
    ColumnarMapFilterOperator op = new ColumnarMapFilterOperator(reader, predicate, "color", 4);

    ImmutableRoaringBitmap result = op.getBitmaps().reduce();
    assertEquals(result.getCardinality(), 2, "Expected exactly 2 matching docs: {1, 3}");
    assertFalse(result.contains(2), "doc 2 (color absent) must be excluded");
    assertFalse(result.contains(0), "doc 0 (color=red, in exclusion list) must be excluded");
    assertEquals(result.toArray(), new int[]{1, 3});
  }

  /**
   * NOT_EQ when ALL docs lack the key → empty result.
   * Verifies that absence-exclusion applies to every doc when none carry the key.
   */
  @Test
  public void testNotEqAllDocsAbsentReturnsEmpty()
      throws Exception {
    String colName = "color_all_absent";
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(colName);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 10);
    OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(_tmpDir, colName, fieldSpec, config);
    // 3 docs, none have the "color" key
    creator.add(Collections.emptyMap());
    creator.add(Collections.emptyMap());
    creator.add(Collections.emptyMap());
    creator.seal();
    creator.close();

    File indexFile = new File(_tmpDir, colName + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buf, null);

    NotEqPredicate predicate = new NotEqPredicate(ExpressionContext.forIdentifier("color"), "red");
    ColumnarMapFilterOperator op = new ColumnarMapFilterOperator(reader, predicate, "color", 3);

    ImmutableRoaringBitmap result = op.getBitmaps().reduce();
    assertEquals(result.getCardinality(), 0, "All docs absent → empty result for NOT_EQ");
  }

  /**
   * IS_NOT_NULL: returns docs where the key is present.
   * color IS NOT NULL → {0, 1, 3} (excludes doc 2 which has no color key)
   */
  @Test
  public void testIsNotNullReturnsOnlyDocsWithKey()
      throws Exception {
    ImmutableColumnarMapIndexReader reader = buildColorIndex("color_is_not_null");
    IsNotNullPredicate predicate = new IsNotNullPredicate(ExpressionContext.forIdentifier("color"));
    ColumnarMapFilterOperator op = new ColumnarMapFilterOperator(reader, predicate, "color", 4);

    ImmutableRoaringBitmap result = op.getBitmaps().reduce();
    assertEquals(result.getCardinality(), 3, "Expected exactly 3 matching docs: {0, 1, 3}");
    assertTrue(result.contains(0), "doc 0 (color=red) must be included");
    assertTrue(result.contains(1), "doc 1 (color=blue) must be included");
    assertFalse(result.contains(2), "doc 2 (color absent) must be excluded");
    assertTrue(result.contains(3), "doc 3 (color=green) must be included");
    assertEquals(result.toArray(), new int[]{0, 1, 3});
  }

  /**
   * IS_NULL: returns docs where the key is absent.
   * color IS NULL → {2} (only doc 2 has no color key)
   */
  @Test
  public void testIsNullReturnsOnlyDocsWithoutKey()
      throws Exception {
    ImmutableColumnarMapIndexReader reader = buildColorIndex("color_is_null");
    IsNullPredicate predicate = new IsNullPredicate(ExpressionContext.forIdentifier("color"));
    ColumnarMapFilterOperator op = new ColumnarMapFilterOperator(reader, predicate, "color", 4);

    ImmutableRoaringBitmap result = op.getBitmaps().reduce();
    assertEquals(result.getCardinality(), 1, "Expected exactly 1 matching doc: {2}");
    assertTrue(result.contains(2), "doc 2 (color absent) must be included");
    assertFalse(result.contains(0), "doc 0 (color=red) must be excluded");
    assertEquals(result.toArray(), new int[]{2});
  }

  /**
   * IS_NOT_NULL on a key that no doc has → empty result.
   */
  @Test
  public void testIsNotNullOnAbsentKeyReturnsEmpty()
      throws Exception {
    ImmutableColumnarMapIndexReader reader = buildColorIndex("color_absent_key");
    IsNotNullPredicate predicate = new IsNotNullPredicate(ExpressionContext.forIdentifier("nonexistent_key"));
    ColumnarMapFilterOperator op = new ColumnarMapFilterOperator(reader, predicate, "nonexistent_key", 4);

    ImmutableRoaringBitmap result = op.getBitmaps().reduce();
    assertEquals(result.getCardinality(), 0, "No doc has 'nonexistent_key' → empty result");
  }

  /**
   * IS_NULL on a key that every doc has → empty result.
   * Build a 3-doc index where all docs have the key.
   */
  @Test
  public void testIsNullOnFullPresenceReturnsEmpty()
      throws Exception {
    String colName = "color_full_presence";
    ComplexFieldSpec fieldSpec = buildMapFieldSpec(colName);
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true, null, true, null, 10);
    OnHeapColumnarMapIndexCreator creator = new OnHeapColumnarMapIndexCreator(_tmpDir, colName, fieldSpec, config);
    Map<String, Object> doc = new HashMap<>();
    doc.put("color", "red");
    creator.add(doc);
    doc.clear();
    doc.put("color", "blue");
    creator.add(doc);
    doc.clear();
    doc.put("color", "green");
    creator.add(doc);
    creator.seal();
    creator.close();

    File indexFile = new File(_tmpDir, colName + V1Constants.Indexes.COLUMNAR_MAP_INDEX_FILE_EXTENSION);
    PinotDataBuffer buf = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
    ImmutableColumnarMapIndexReader reader = new ImmutableColumnarMapIndexReader(buf, null);

    IsNullPredicate predicate = new IsNullPredicate(ExpressionContext.forIdentifier("color"));
    ColumnarMapFilterOperator op = new ColumnarMapFilterOperator(reader, predicate, "color", 3);

    ImmutableRoaringBitmap result = op.getBitmaps().reduce();
    assertEquals(result.getCardinality(), 0, "All docs have 'color' → IS NULL returns empty");
  }
}
