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
package org.apache.pinot.core.plan;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.MapDataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;


/**
 * Tests for {@link AggregationPlanNode} MAP key resolution methods.
 */
public class AggregationPlanNodeTest {

  @Test
  public void testTryResolveMapKeyDataSourceWithItemExpression()
      throws Exception {
    // Build item(mapCol, 'keyName') expression
    ExpressionContext mapColArg = ExpressionContext.forIdentifier("mapCol");
    ExpressionContext keyArg = ExpressionContext.forLiteral(DataType.STRING, "keyName");
    FunctionContext itemFunc = new FunctionContext(FunctionContext.Type.TRANSFORM, "item",
        Arrays.asList(mapColArg, keyArg));
    ExpressionContext itemExpr = ExpressionContext.forFunction(itemFunc);

    DataSource perKeyDs = mock(DataSource.class);
    Dictionary dict = mock(Dictionary.class);
    when(perKeyDs.getDictionary()).thenReturn(dict);

    MapDataSource mapDs = mock(MapDataSource.class);
    when(mapDs.getKeyDataSource("keyName")).thenReturn(perKeyDs);

    IndexSegment segment = mock(IndexSegment.class);
    when(segment.getDataSource(eq("mapCol"), any())).thenReturn(mapDs);

    QueryContext queryContext = mock(QueryContext.class);
    SegmentContext segmentContext = new SegmentContext(segment);

    AggregationPlanNode planNode = new AggregationPlanNode(segmentContext, queryContext);

    Method method = AggregationPlanNode.class.getDeclaredMethod("tryResolveMapKeyDataSource",
        ExpressionContext.class);
    method.setAccessible(true);

    DataSource result = (DataSource) method.invoke(planNode, itemExpr);
    assertNotNull(result);
    assertSame(result, perKeyDs);
  }

  @Test
  public void testTryResolveMapKeyDataSourceReturnsNullForNonItemFunction()
      throws Exception {
    // Build add(a, b) expression — not an item() call
    ExpressionContext aArg = ExpressionContext.forIdentifier("a");
    ExpressionContext bArg = ExpressionContext.forIdentifier("b");
    FunctionContext addFunc = new FunctionContext(FunctionContext.Type.TRANSFORM, "add",
        Arrays.asList(aArg, bArg));
    ExpressionContext addExpr = ExpressionContext.forFunction(addFunc);

    IndexSegment segment = mock(IndexSegment.class);
    QueryContext queryContext = mock(QueryContext.class);
    SegmentContext segmentContext = new SegmentContext(segment);

    AggregationPlanNode planNode = new AggregationPlanNode(segmentContext, queryContext);

    Method method = AggregationPlanNode.class.getDeclaredMethod("tryResolveMapKeyDataSource",
        ExpressionContext.class);
    method.setAccessible(true);

    DataSource result = (DataSource) method.invoke(planNode, addExpr);
    assertNull(result);
  }

  @Test
  public void testTryResolveMapKeyDataSourceReturnsNullForNonMapColumn()
      throws Exception {
    // Build item(regularCol, 'key') where regularCol is not a MapDataSource
    ExpressionContext colArg = ExpressionContext.forIdentifier("regularCol");
    ExpressionContext keyArg = ExpressionContext.forLiteral(DataType.STRING, "key");
    FunctionContext itemFunc = new FunctionContext(FunctionContext.Type.TRANSFORM, "item",
        Arrays.asList(colArg, keyArg));
    ExpressionContext itemExpr = ExpressionContext.forFunction(itemFunc);

    DataSource regularDs = mock(DataSource.class); // not a MapDataSource
    IndexSegment segment = mock(IndexSegment.class);
    when(segment.getDataSource(eq("regularCol"), any())).thenReturn(regularDs);

    QueryContext queryContext = mock(QueryContext.class);
    SegmentContext segmentContext = new SegmentContext(segment);

    AggregationPlanNode planNode = new AggregationPlanNode(segmentContext, queryContext);

    Method method = AggregationPlanNode.class.getDeclaredMethod("tryResolveMapKeyDataSource",
        ExpressionContext.class);
    method.setAccessible(true);

    DataSource result = (DataSource) method.invoke(planNode, itemExpr);
    assertNull(result);
  }

  @Test
  public void testResolveDataSourceWithIdentifier()
      throws Exception {
    DataSource ds = mock(DataSource.class);
    IndexSegment segment = mock(IndexSegment.class);
    when(segment.getDataSource(eq("col1"), any())).thenReturn(ds);

    QueryContext queryContext = mock(QueryContext.class);
    SegmentContext segmentContext = new SegmentContext(segment);

    AggregationPlanNode planNode = new AggregationPlanNode(segmentContext, queryContext);

    Method method = AggregationPlanNode.class.getDeclaredMethod("resolveDataSource",
        ExpressionContext.class);
    method.setAccessible(true);

    ExpressionContext identExpr = ExpressionContext.forIdentifier("col1");
    DataSource result = (DataSource) method.invoke(planNode, identExpr);
    assertSame(result, ds);
  }

  @Test
  public void testResolveDataSourceReturnsNullForLiteral()
      throws Exception {
    IndexSegment segment = mock(IndexSegment.class);
    QueryContext queryContext = mock(QueryContext.class);
    SegmentContext segmentContext = new SegmentContext(segment);

    AggregationPlanNode planNode = new AggregationPlanNode(segmentContext, queryContext);

    Method method = AggregationPlanNode.class.getDeclaredMethod("resolveDataSource",
        ExpressionContext.class);
    method.setAccessible(true);

    ExpressionContext literalExpr = ExpressionContext.forLiteral(DataType.INT, 42);
    DataSource result = (DataSource) method.invoke(planNode, literalExpr);
    assertNull(result);
  }

  @Test
  public void testTryResolveMapKeyDataSourceReturnsNullForWrongArgCount()
      throws Exception {
    // Build item(mapCol) — only 1 arg instead of 2
    ExpressionContext mapColArg = ExpressionContext.forIdentifier("mapCol");
    FunctionContext itemFunc = new FunctionContext(FunctionContext.Type.TRANSFORM, "item",
        Collections.singletonList(mapColArg));
    ExpressionContext itemExpr = ExpressionContext.forFunction(itemFunc);

    IndexSegment segment = mock(IndexSegment.class);
    QueryContext queryContext = mock(QueryContext.class);
    SegmentContext segmentContext = new SegmentContext(segment);

    AggregationPlanNode planNode = new AggregationPlanNode(segmentContext, queryContext);

    Method method = AggregationPlanNode.class.getDeclaredMethod("tryResolveMapKeyDataSource",
        ExpressionContext.class);
    method.setAccessible(true);

    DataSource result = (DataSource) method.invoke(planNode, itemExpr);
    assertNull(result);
  }

  @Test
  public void testResolveDataSourceForItemExprReturnsDataSourceWithDictionary()
      throws Exception {
    // Verifies the full isFitForNonScanBasedPlan path: resolveDataSource for item(mapCol, 'key')
    // returns a DataSource whose getDictionary() is non-null.
    ExpressionContext mapColArg = ExpressionContext.forIdentifier("mapCol");
    ExpressionContext keyArg = ExpressionContext.forLiteral(DataType.STRING, "myKey");
    FunctionContext itemFunc = new FunctionContext(FunctionContext.Type.TRANSFORM, "item",
        Arrays.asList(mapColArg, keyArg));
    ExpressionContext itemExpr = ExpressionContext.forFunction(itemFunc);

    Dictionary dict = mock(Dictionary.class);
    DataSource perKeyDs = mock(DataSource.class);
    when(perKeyDs.getDictionary()).thenReturn(dict);

    MapDataSource mapDs = mock(MapDataSource.class);
    when(mapDs.getKeyDataSource("myKey")).thenReturn(perKeyDs);

    IndexSegment segment = mock(IndexSegment.class);
    when(segment.getDataSource(eq("mapCol"), any())).thenReturn(mapDs);

    QueryContext queryContext = mock(QueryContext.class);
    SegmentContext segmentContext = new SegmentContext(segment);

    AggregationPlanNode planNode = new AggregationPlanNode(segmentContext, queryContext);

    Method method = AggregationPlanNode.class.getDeclaredMethod("resolveDataSource",
        ExpressionContext.class);
    method.setAccessible(true);

    DataSource result = (DataSource) method.invoke(planNode, itemExpr);
    assertNotNull(result, "resolveDataSource should return non-null for item() on a MapDataSource");
    assertNotNull(result.getDictionary(),
        "Per-key DataSource should have a non-null dictionary (enables NonScanBasedAggregationOperator)");
  }

  @Test
  public void testResolveDataSourceForItemExprHasPositiveCardinality()
      throws Exception {
    // Verifies that the per-key DataSource returned by resolveDataSource has cardinality > 0,
    // which is required for dictionary-based aggregation functions.
    ExpressionContext mapColArg = ExpressionContext.forIdentifier("mapCol");
    ExpressionContext keyArg = ExpressionContext.forLiteral(DataType.STRING, "status");
    FunctionContext itemFunc = new FunctionContext(FunctionContext.Type.TRANSFORM, "item",
        Arrays.asList(mapColArg, keyArg));
    ExpressionContext itemExpr = ExpressionContext.forFunction(itemFunc);

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getCardinality()).thenReturn(5);

    Dictionary dict = mock(Dictionary.class);
    DataSource perKeyDs = mock(DataSource.class);
    when(perKeyDs.getDictionary()).thenReturn(dict);
    when(perKeyDs.getDataSourceMetadata()).thenReturn(metadata);

    MapDataSource mapDs = mock(MapDataSource.class);
    when(mapDs.getKeyDataSource("status")).thenReturn(perKeyDs);

    IndexSegment segment = mock(IndexSegment.class);
    when(segment.getDataSource(eq("mapCol"), any())).thenReturn(mapDs);

    QueryContext queryContext = mock(QueryContext.class);
    SegmentContext segmentContext = new SegmentContext(segment);

    AggregationPlanNode planNode = new AggregationPlanNode(segmentContext, queryContext);

    Method method = AggregationPlanNode.class.getDeclaredMethod("resolveDataSource",
        ExpressionContext.class);
    method.setAccessible(true);

    DataSource result = (DataSource) method.invoke(planNode, itemExpr);
    assertNotNull(result);
    assert result.getDataSourceMetadata().getCardinality() > 0
        : "Per-key DataSource should have positive cardinality";
  }
}
