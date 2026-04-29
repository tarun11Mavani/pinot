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
package org.apache.pinot.spi.data;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class ColumnarMapNamingTest {

  @Test
  public void testVirtualColumnName() {
    assertEquals(ColumnarMapNaming.virtualColumnName("metrics", "tenancy"), "metrics$__tenancy");
  }

  @Test
  public void testSparseColumnName() {
    assertEquals(ColumnarMapNaming.sparseColumnName("metrics"), "metrics$____sparse__");
  }

  @Test
  public void testIsVirtualColumn() {
    assertTrue(ColumnarMapNaming.isColumnarMapVirtualColumn("metrics$__tenancy"));
    assertTrue(ColumnarMapNaming.isColumnarMapVirtualColumn("metrics$____sparse__"));
    assertFalse(ColumnarMapNaming.isColumnarMapVirtualColumn("metrics"));
    assertFalse(ColumnarMapNaming.isColumnarMapVirtualColumn("normal_column"));
    assertFalse(ColumnarMapNaming.isColumnarMapVirtualColumn("metrics$$tenancy"));
  }

  @Test
  public void testParseMapColumn() {
    assertEquals(ColumnarMapNaming.parseMapColumn("metrics$__tenancy"), "metrics");
    assertEquals(ColumnarMapNaming.parseMapColumn("m__data$__key$__nested"), "m__data");
  }

  @Test
  public void testParseKey() {
    assertEquals(ColumnarMapNaming.parseKey("metrics$__tenancy"), "tenancy");
    assertEquals(ColumnarMapNaming.parseKey("metrics$____sparse__"), "__sparse__");
  }

  @Test
  public void testIsSparseColumn() {
    assertTrue(ColumnarMapNaming.isSparseColumn("metrics$____sparse__"));
    assertFalse(ColumnarMapNaming.isSparseColumn("metrics$__tenancy"));
  }

  @Test
  public void testRoundTrip() {
    String mapCol = "event_props";
    String key = "country_iso2";
    String virtual = ColumnarMapNaming.virtualColumnName(mapCol, key);
    assertEquals(ColumnarMapNaming.parseMapColumn(virtual), mapCol);
    assertEquals(ColumnarMapNaming.parseKey(virtual), key);
    assertTrue(ColumnarMapNaming.isColumnarMapVirtualColumn(virtual));
    assertFalse(ColumnarMapNaming.isSparseColumn(virtual));
  }
}
