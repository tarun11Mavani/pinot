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


public class MapNamingTest {

  @Test
  public void testMaterializedColumnName() {
    assertEquals(MapNaming.materializedColumnName("metrics", "tenancy"), "metrics$__tenancy");
  }

  @Test
  public void testSparseColumnName() {
    assertEquals(MapNaming.sparseColumnName("metrics"), "metrics$____sparse__");
  }

  @Test
  public void testIsMaterializedMapColumn() {
    assertTrue(MapNaming.isMaterializedMapColumn("metrics$__tenancy"));
    assertTrue(MapNaming.isMaterializedMapColumn("metrics$____sparse__"));
    assertFalse(MapNaming.isMaterializedMapColumn("metrics"));
    assertFalse(MapNaming.isMaterializedMapColumn("normal_column"));
    assertFalse(MapNaming.isMaterializedMapColumn("metrics$$tenancy"));
  }

  @Test
  public void testParseMapColumn() {
    assertEquals(MapNaming.parseMapColumn("metrics$__tenancy"), "metrics");
    assertEquals(MapNaming.parseMapColumn("m__data$__key$__nested"), "m__data");
  }

  @Test
  public void testParseKey() {
    assertEquals(MapNaming.parseKey("metrics$__tenancy"), "tenancy");
    assertEquals(MapNaming.parseKey("metrics$____sparse__"), "__sparse__");
  }

  @Test
  public void testIsSparseColumn() {
    assertTrue(MapNaming.isSparseColumn("metrics$____sparse__"));
    assertFalse(MapNaming.isSparseColumn("metrics$__tenancy"));
  }

  @Test
  public void testRoundTrip() {
    String mapCol = "event_props";
    String key = "country_iso2";
    String materialized = MapNaming.materializedColumnName(mapCol, key);
    assertEquals(MapNaming.parseMapColumn(materialized), mapCol);
    assertEquals(MapNaming.parseKey(materialized), key);
    assertTrue(MapNaming.isMaterializedMapColumn(materialized));
    assertFalse(MapNaming.isSparseColumn(materialized));
  }
}
