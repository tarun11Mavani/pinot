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
package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import java.util.Map;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.spi.Constants;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IntegerTupleSketchAggregationFunctionTest {

  @Test
  public void testCanUseStarTreeDefaultK() {
    IntegerTupleSketchAggregationFunction function =
        new IntegerTupleSketchAggregationFunction(List.of(ExpressionContext.forIdentifier("col")),
            IntegerSummary.Mode.Sum);

    Assert.assertTrue(function.canUseStarTree(Map.of()));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "16384")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, 16384)));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, 8192)));
  }

  @Test
  public void testCanUseCustomK() {
    IntegerTupleSketchAggregationFunction function = new IntegerTupleSketchAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"), ExpressionContext.forLiteral(Literal.intValue(32768))),
        IntegerSummary.Mode.Sum);

    // Default StarTree lgK = 14 / K=16384
    Assert.assertFalse(function.canUseStarTree(Map.of()));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "16384")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "65536")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, 32768)));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "32768")));
  }
}
