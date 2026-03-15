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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.SparseMapFieldSpec;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test for the SPARSE_MAP column type.
 *
 * <p>Starts a standalone Pinot cluster (ZooKeeper, Controller, Broker, Server), ingests
 * a {@code userMetrics} table from JSON records with a {@code metrics} SPARSE_MAP column,
 * and validates currently-supported queries:
 * <ul>
 *   <li>COUNT(*) over the full table.</li>
 *   <li>Projection of regular (non-SPARSE_MAP) columns.</li>
 *   <li>GROUP BY / aggregation on regular columns.</li>
 *   <li>WHERE filtering on SPARSE_MAP key values via the sparse-map inverted index
 *       ({@code SparseMapFilterOperator}).</li>
 * </ul>
 *
 * <p>Queries that require projection of SPARSE_MAP key values (e.g.
 * {@code SELECT metrics['clicks']}) are intentionally excluded until the corresponding
 * {@code MapDataSource} implementation is completed (Task 15 in the design plan).
 */
public class SparseMapClusterIntegrationTest extends BaseClusterIntegrationTest {

  private static final String TABLE_NAME = "userMetrics";

  /** 20 JSON records matching pinot-tools/src/main/resources/examples/batch/userMetrics */
  private static final String[] JSON_RECORDS = {
      "{\"userId\":\"u001\",\"region\":\"US\",\"ts\":1700000000000,"
          + "\"metrics\":{\"clicks\":42,\"spend\":12.50,\"country\":\"US\"}}",
      "{\"userId\":\"u002\",\"region\":\"EU\",\"ts\":1700000060000,"
          + "\"metrics\":{\"sessions\":5,\"country\":\"DE\"}}",
      "{\"userId\":\"u003\",\"region\":\"US\",\"ts\":1700000120000,"
          + "\"metrics\":{\"clicks\":7}}",
      "{\"userId\":\"u004\",\"region\":\"APAC\",\"ts\":1700000180000,"
          + "\"metrics\":{\"spend\":99.99,\"country\":\"JP\"}}",
      "{\"userId\":\"u005\",\"region\":\"US\",\"ts\":1700000240000,"
          + "\"metrics\":{\"clicks\":150,\"spend\":45.00,\"sessions\":12,\"country\":\"US\"}}",
      "{\"userId\":\"u006\",\"region\":\"EU\",\"ts\":1700000300000,"
          + "\"metrics\":{\"clicks\":3,\"country\":\"FR\"}}",
      "{\"userId\":\"u007\",\"region\":\"APAC\",\"ts\":1700000360000,"
          + "\"metrics\":{\"sessions\":1}}",
      "{\"userId\":\"u008\",\"region\":\"US\",\"ts\":1700000420000,"
          + "\"metrics\":{\"clicks\":88,\"spend\":23.75}}",
      "{\"userId\":\"u009\",\"region\":\"EU\",\"ts\":1700000480000,"
          + "\"metrics\":{\"spend\":7.20,\"country\":\"UK\"}}",
      "{\"userId\":\"u010\",\"region\":\"US\",\"ts\":1700000540000,"
          + "\"metrics\":{\"clicks\":210,\"sessions\":30,\"country\":\"US\"}}",
      "{\"userId\":\"u011\",\"region\":\"APAC\",\"ts\":1700000600000,"
          + "\"metrics\":{\"clicks\":15,\"spend\":5.00,\"country\":\"AU\"}}",
      "{\"userId\":\"u012\",\"region\":\"EU\",\"ts\":1700000660000,"
          + "\"metrics\":{\"country\":\"DE\",\"sessions\":8}}",
      "{\"userId\":\"u013\",\"region\":\"US\",\"ts\":1700000720000,"
          + "\"metrics\":{\"clicks\":330,\"spend\":110.00,\"sessions\":55}}",
      "{\"userId\":\"u014\",\"region\":\"APAC\",\"ts\":1700000780000,"
          + "\"metrics\":{\"spend\":18.40}}",
      "{\"userId\":\"u015\",\"region\":\"EU\",\"ts\":1700000840000,"
          + "\"metrics\":{\"clicks\":62,\"country\":\"ES\"}}",
      "{\"userId\":\"u016\",\"region\":\"US\",\"ts\":1700000900000,"
          + "\"metrics\":{\"sessions\":3,\"country\":\"US\"}}",
      "{\"userId\":\"u017\",\"region\":\"APAC\",\"ts\":1700000960000,"
          + "\"metrics\":{\"clicks\":5,\"sessions\":2,\"country\":\"IN\"}}",
      "{\"userId\":\"u018\",\"region\":\"EU\",\"ts\":1700001020000,"
          + "\"metrics\":{\"clicks\":19,\"spend\":4.80,\"country\":\"IT\"}}",
      "{\"userId\":\"u019\",\"region\":\"US\",\"ts\":1700001080000,"
          + "\"metrics\":{\"spend\":250.00,\"country\":\"US\"}}",
      "{\"userId\":\"u020\",\"region\":\"APAC\",\"ts\":1700001140000,"
          + "\"metrics\":{\"clicks\":77,\"spend\":31.50,\"sessions\":9,\"country\":\"SG\"}}"
  };

  // Expected counts derived from the JSON_RECORDS above:
  // US region: u001, u003, u005, u008, u010, u013, u016, u019 = 8 records
  // EU region: u002, u006, u009, u012, u015, u018 = 6 records
  // APAC region: u004, u007, u011, u014, u017, u020 = 6 records
  // metrics['country'] = 'US': u001, u005, u010, u016, u019 = 5 records
  // metrics['country'] IN ('US', 'DE'): above 5 + u002, u012 = 7 records
  // metrics['country'] != 'US': 15 docs have 'country' key, minus 5 with country='US' = 10
  // metrics['clicks'] >= 5: u001(42), u003(7), u005(150), u008(88), u010(210), u011(15),
  //                         u013(330), u015(62), u017(5), u018(19), u020(77) = 11 records
  // metrics['clicks'] BETWEEN 10 AND 100: u001(42), u008(88), u011(15), u015(62), u018(19),
  //                                       u020(77) = 6 records
  // SUM(metrics['clicks']): 42+7+150+3+88+210+15+330+62+5+19+77 = 1008 (absent = 0)
  private static final long TOTAL_DOCS = 20;
  private static final long US_REGION_COUNT = 8;
  private static final long EU_REGION_COUNT = 6;
  private static final long APAC_REGION_COUNT = 6;
  private static final long COUNTRY_US_COUNT = 5;
  private static final long COUNTRY_US_OR_DE_COUNT = 7;
  private static final long COUNTRY_NOT_US_COUNT = 10;
  private static final long CLICKS_GTE_5_COUNT = 11;
  private static final long CLICKS_BETWEEN_10_AND_100_COUNT = 6;
  private static final long CLICKS_GTE_100_COUNT = 3;  // u005(150), u010(210), u013(330)
  private static final double CLICKS_SUM = 1008.0;

  // IS_NOT_NULL / IS_NULL counts derived from the JSON_RECORDS:
  // clicks present: u001,u003,u005,u006,u008,u010,u011,u013,u015,u017,u018,u020 = 12
  // clicks absent: u002,u004,u007,u009,u012,u014,u016,u019 = 8
  // country present: u001,u002,u004,u005,u006,u009,u010,u011,u012,u015,u016,u017,u018,u019,u020 = 15
  // country absent: u003,u007,u008,u013,u014 = 5
  private static final long CLICKS_NOT_NULL_COUNT = 12;
  private static final long CLICKS_NULL_COUNT = 8;
  private static final long COUNTRY_NOT_NULL_COUNT = 15;
  private static final long COUNTRY_NULL_COUNT = 5;

  @Override
  protected long getCountStarResult() {
    return TOTAL_DOCS;
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startController();
    startBroker();
    startServer();

    Schema schema = buildSparseMapSchema();
    addSchema(schema);

    TableConfig tableConfig = buildSparseMapTableConfig();
    addTableConfig(tableConfig);

    File jsonFile = writeJsonData();
    ClusterIntegrationTestUtils.buildSegmentFromFile(jsonFile, tableConfig, schema, "0", _segmentDir, _tarDir,
        FileFormat.JSON);
    uploadSegments(getTableName(), _tarDir);

    waitForAllDocsLoaded(60_000);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  // -------------------------------------------------------------------------
  // Schema and table config helpers
  // -------------------------------------------------------------------------

  private Schema buildSparseMapSchema() {
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("clicks", FieldSpec.DataType.LONG);
    keyTypes.put("spend", FieldSpec.DataType.DOUBLE);
    keyTypes.put("sessions", FieldSpec.DataType.INT);
    keyTypes.put("country", FieldSpec.DataType.STRING);

    SparseMapFieldSpec metricsField = new SparseMapFieldSpec("metrics", keyTypes);
    metricsField.setDefaultValueType(FieldSpec.DataType.STRING);

    return new Schema.SchemaBuilder()
        .setSchemaName(TABLE_NAME)
        .addSingleValueDimension("userId", FieldSpec.DataType.STRING)
        .addSingleValueDimension("region", FieldSpec.DataType.STRING)
        .addField(metricsField)
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  private TableConfig buildSparseMapTableConfig() {
    ObjectNode sparseMapNode = JsonUtils.newObjectNode();
    sparseMapNode.put("enabled", true);
    sparseMapNode.put("enableInvertedIndexForAll", true);
    sparseMapNode.put("maxKeys", 100);
    ObjectNode indexesNode = JsonUtils.newObjectNode();
    indexesNode.set("sparse_map", sparseMapNode);
    FieldConfig metricsFieldConfig = new FieldConfig.Builder("metrics")
        .withIndexes(indexesNode)
        .build();

    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setTimeColumnName("ts")
        .setFieldConfigList(List.of(metricsFieldConfig))
        .build();
  }

  /** Writes the JSON test records to a temporary file and returns it. */
  private File writeJsonData()
      throws Exception {
    File jsonFile = new File(_tempDir, "userMetrics_data.json");
    StringBuilder sb = new StringBuilder();
    for (String record : JSON_RECORDS) {
      sb.append(record).append('\n');
    }
    FileUtils.writeStringToFile(jsonFile, sb.toString(), java.nio.charset.StandardCharsets.UTF_8);
    return jsonFile;
  }

  // -------------------------------------------------------------------------
  // Test cases
  // -------------------------------------------------------------------------

  /** COUNT(*) must return all 20 rows. */
  @Test
  public void testCountStar()
      throws Exception {
    JsonNode response = postQuery("SELECT COUNT(*) FROM " + TABLE_NAME);
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    long count = response.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(count, TOTAL_DOCS);
  }

  /** Regular-column projection must work without errors. */
  @Test
  public void testRegularColumnProjection()
      throws Exception {
    JsonNode response = postQuery("SELECT userId, region FROM " + TABLE_NAME + " LIMIT 20");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    JsonNode rows = response.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), (int) TOTAL_DOCS);
    // Every row must have a non-null userId and region
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertNotNull(rows.get(i).get(0).asText());
      Assert.assertNotNull(rows.get(i).get(1).asText());
    }
  }

  /** Filtering on a regular column must return the correct subset. */
  @Test
  public void testRegularColumnFilter()
      throws Exception {
    JsonNode response = postQuery("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE region = 'US'");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    long count = response.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(count, US_REGION_COUNT);
  }

  /** GROUP BY on a regular column must return per-region counts. */
  @Test
  public void testGroupByRegion()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT region, COUNT(*) FROM " + TABLE_NAME + " GROUP BY region ORDER BY region");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    JsonNode rows = response.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), 3);

    // Rows are ordered alphabetically: APAC, EU, US
    Assert.assertEquals(rows.get(0).get(0).asText(), "APAC");
    Assert.assertEquals(rows.get(0).get(1).longValue(), APAC_REGION_COUNT);

    Assert.assertEquals(rows.get(1).get(0).asText(), "EU");
    Assert.assertEquals(rows.get(1).get(1).longValue(), EU_REGION_COUNT);

    Assert.assertEquals(rows.get(2).get(0).asText(), "US");
    Assert.assertEquals(rows.get(2).get(1).longValue(), US_REGION_COUNT);
  }

  /**
   * Filtering on a SPARSE_MAP key value must use the sparse-map inverted index
   * ({@code SparseMapFilterOperator}) and return the correct count.
   *
   * <p>Records with {@code metrics['country'] = 'US'}: u001, u005, u010, u016, u019 → 5 rows.
   */
  @Test
  public void testSparseMapKeyFilter()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE metrics['country'] = 'US'");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    long count = response.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(count, COUNTRY_US_COUNT);
  }

  /**
   * Filtering on a SPARSE_MAP key combined with a regular-column filter must intersect correctly.
   *
   * <p>Records with {@code region = 'US'} AND {@code metrics['country'] = 'US'}:
   * u001, u005, u010, u016, u019 (all US-region rows with country=US are within the US region
   * so the result is still 5).
   */
  @Test
  public void testSparseMapKeyFilterAndRegularColumnFilter()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT COUNT(*) FROM " + TABLE_NAME
            + " WHERE region = 'US' AND metrics['country'] = 'US'");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    long count = response.get("resultTable").get("rows").get(0).get(0).longValue();
    // u001, u005, u010, u016, u019 all have region=US and country=US
    Assert.assertEquals(count, COUNTRY_US_COUNT);
  }

  /**
   * Filtering for a SPARSE_MAP key value that matches zero rows must return 0.
   *
   * <p>No record has {@code metrics['country'] = 'ZZ'}.
   */
  @Test
  public void testSparseMapKeyFilterNoMatch()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE metrics['country'] = 'ZZ'");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    long count = response.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(count, 0L);
  }

  /**
   * SELECT on regular columns filtered by a SPARSE_MAP key value must project the right rows.
   *
   * <p>Rows with {@code metrics['country'] = 'US'}: u001, u005, u010, u016, u019.
   * All are in the US region.
   */
  @Test
  public void testProjectRegularColumnsWithSparseMapFilter()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT userId, region FROM " + TABLE_NAME
            + " WHERE metrics['country'] = 'US' ORDER BY userId");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    JsonNode rows = response.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), (int) COUNTRY_US_COUNT);

    // All returned rows must belong to the US region
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(rows.get(i).get(1).asText(), "US",
          "Expected region=US for row " + i + " but got: " + rows.get(i));
    }
  }

  /**
   * Filtering on a numeric SPARSE_MAP key (LONG type) must work correctly.
   *
   * <p>Records with {@code metrics['clicks'] = 42}: u001 → 1 row.
   */
  @Test
  public void testSparseMapNumericKeyFilter()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE metrics['clicks'] = '42'");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    long count = response.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(count, 1L);
  }

  /**
   * Projecting a SPARSE_MAP key in the SELECT clause must return per-row values.
   *
   * <p>All 20 rows are returned; docs that do not contain the 'clicks' key return 0 (the
   * LONG zero-default). Row for u001 must have clicks = 42.
   */
  @Test
  public void testSparseMapKeyProjection()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT userId, metrics['clicks'] FROM " + TABLE_NAME + " ORDER BY userId LIMIT 20");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    JsonNode rows = response.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), (int) TOTAL_DOCS);

    // u001 must have clicks = 42
    boolean foundU001 = false;
    for (int i = 0; i < rows.size(); i++) {
      if ("u001".equals(rows.get(i).get(0).asText())) {
        Assert.assertEquals(rows.get(i).get(1).longValue(), 42L,
            "Expected clicks=42 for u001 but got: " + rows.get(i));
        foundU001 = true;
      }
    }
    Assert.assertTrue(foundU001, "Row for u001 not found in result");
  }

  /**
   * Projecting multiple SPARSE_MAP keys for a single row must return the correct typed values.
   *
   * <p>u005 has clicks=150 and spend=45.0.
   */
  @Test
  public void testMultipleSparseMapKeyProjection()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT userId, metrics['clicks'], metrics['spend'] FROM " + TABLE_NAME
            + " WHERE userId = 'u005' LIMIT 1");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    JsonNode rows = response.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), 1);
    Assert.assertEquals(rows.get(0).get(1).longValue(), 150L, "Expected clicks=150 for u005");
    Assert.assertEquals(rows.get(0).get(2).doubleValue(), 45.0, 0.001,
        "Expected spend=45.0 for u005");
  }

  /**
   * Range predicate ({@code >=}) on a numeric SPARSE_MAP key must return matching rows.
   * Docs without the key contribute the zero-default (0L) and do not match {@code >= 5}.
   *
   * <p>Records with clicks ≥ 5: u001, u003, u005, u008, u010, u011, u013, u015, u017, u018,
   * u020 → 11 rows.
   */
  @Test
  public void testSparseMapRangeFilter()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE metrics['clicks'] >= 5");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    long count = response.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(count, CLICKS_GTE_5_COUNT);
  }

  /**
   * BETWEEN predicate on a numeric SPARSE_MAP key must return only rows in the inclusive range.
   *
   * <p>Records with clicks in [10, 100]: u001(42), u008(88), u011(15), u015(62), u018(19),
   * u020(77) → 6 rows.
   */
  @Test
  public void testSparseMapBetweenFilter()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE metrics['clicks'] BETWEEN 10 AND 100");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    long count = response.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(count, CLICKS_BETWEEN_10_AND_100_COUNT);
  }

  /**
   * IN predicate on a string SPARSE_MAP key uses the inverted index for fast lookup.
   *
   * <p>Records with country in ('US', 'DE'): u001, u002, u005, u010, u012, u016, u019 → 7 rows.
   */
  @Test
  public void testSparseMapInFilter()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE metrics['country'] IN ('US', 'DE')");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    long count = response.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(count, COUNTRY_US_OR_DE_COUNT);
  }

  /**
   * NOT_EQ predicate on a SPARSE_MAP key uses the inverted index: returns docs that
   * <em>have</em> the key but whose value is not equal to the target.
   *
   * <p>15 docs have a 'country' value; 5 of them equal 'US' → 10 rows returned.
   */
  @Test
  public void testSparseMapNotEqFilter()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE metrics['country'] != 'US'");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    long count = response.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(count, COUNTRY_NOT_US_COUNT);
  }

  /**
   * SUM aggregation over a SPARSE_MAP key must sum the stored values across all documents.
   * Docs that do not have the key contribute the zero-default (0L) to the sum.
   *
   * <p>SUM(clicks) = 42+7+150+3+88+210+15+330+62+5+19+77 = 1008.
   */
  @Test
  public void testSparseMapSumAggregation()
      throws Exception {
    JsonNode response = postQuery("SELECT SUM(metrics['clicks']) FROM " + TABLE_NAME);
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    double sum = response.get("resultTable").get("rows").get(0).get(0).doubleValue();
    Assert.assertEquals(sum, CLICKS_SUM, 0.001);
  }

  /**
   * GROUP BY on a SPARSE_MAP key must group rows by the key value.
   *
   * <p>Using a WHERE filter to limit to known countries avoids the empty-string group
   * that would appear for docs without a 'country' key (absent keys return "").
   * Expected groups: US=5, DE=2, JP=1.
   */
  @Test
  public void testGroupBySparseMapKey()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT metrics['country'], COUNT(*) FROM " + TABLE_NAME
            + " WHERE metrics['country'] IN ('US', 'DE', 'JP')"
            + " GROUP BY metrics['country'] ORDER BY COUNT(*) DESC, metrics['country']");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    JsonNode rows = response.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), 3);

    Assert.assertEquals(rows.get(0).get(0).asText(), "US");
    Assert.assertEquals(rows.get(0).get(1).longValue(), 5L);

    Assert.assertEquals(rows.get(1).get(0).asText(), "DE");
    Assert.assertEquals(rows.get(1).get(1).longValue(), 2L);

    Assert.assertEquals(rows.get(2).get(0).asText(), "JP");
    Assert.assertEquals(rows.get(2).get(1).longValue(), 1L);
  }

  /**
   * Combining SPARSE_MAP key projection with a range filter must return only the matching rows
   * with the correct projected values.
   *
   * <p>clicks ≥ 100: u005(150), u010(210), u013(330) → 3 rows ordered by clicks DESC.
   */
  @Test
  public void testSparseMapKeyProjectionWithRangeFilter()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT userId, metrics['clicks'] FROM " + TABLE_NAME
            + " WHERE metrics['clicks'] >= 100 ORDER BY metrics['clicks'] DESC");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    JsonNode rows = response.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), (int) CLICKS_GTE_100_COUNT);

    // Verify descending order and values
    Assert.assertEquals(rows.get(0).get(0).asText(), "u013");
    Assert.assertEquals(rows.get(0).get(1).longValue(), 330L);
    Assert.assertEquals(rows.get(1).get(0).asText(), "u010");
    Assert.assertEquals(rows.get(1).get(1).longValue(), 210L);
    Assert.assertEquals(rows.get(2).get(0).asText(), "u005");
    Assert.assertEquals(rows.get(2).get(1).longValue(), 150L);
  }

  /**
   * Selecting all non-SPARSE_MAP columns (equivalent of SELECT * excluding the map column) must
   * return all 20 rows with correct values for the regular columns.
   *
   * <p>Note: {@code SELECT *} on a table that contains a SPARSE_MAP column is not yet fully
   * supported because SPARSE_MAP columns have no traditional forward index and cannot be directly
   * projected without the {@code metrics['key']} syntax.
   */
  @Test
  public void testSelectAllNonMapColumns()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT userId, region, ts FROM " + TABLE_NAME + " ORDER BY userId LIMIT 20");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    JsonNode rows = response.get("resultTable").get("rows");
    Assert.assertEquals(rows.size(), (int) TOTAL_DOCS);

    // Spot-check: u001 is in the US region
    boolean foundU001 = false;
    for (int i = 0; i < rows.size(); i++) {
      if ("u001".equals(rows.get(i).get(0).asText())) {
        Assert.assertEquals(rows.get(i).get(1).asText(), "US");
        foundU001 = true;
      }
    }
    Assert.assertTrue(foundU001, "Row for u001 not found");
  }

  /**
   * IS NOT NULL on a SPARSE_MAP key must return docs that have the key present.
   * Uses the presence bitmap — O(1), zero doc scanning.
   */
  @Test
  public void testIsNotNullFilter()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE metrics['clicks'] IS NOT NULL");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    long count = response.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(count, CLICKS_NOT_NULL_COUNT);
  }

  /**
   * IS NULL on a SPARSE_MAP key must return docs where the key is absent.
   * Uses the flipped presence bitmap.
   */
  @Test
  public void testIsNullFilter()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE metrics['clicks'] IS NULL");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    long count = response.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(count, CLICKS_NULL_COUNT);
  }

  /**
   * IS NOT NULL + IS NULL must be complements: their counts must sum to COUNT(*).
   */
  @Test
  public void testIsNotNullPlusIsNullEqualsTotal()
      throws Exception {
    JsonNode notNullResponse = postQuery(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE metrics['country'] IS NOT NULL");
    Assert.assertEquals(notNullResponse.get("exceptions").size(), 0);
    long notNullCount = notNullResponse.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(notNullCount, COUNTRY_NOT_NULL_COUNT);

    JsonNode nullResponse = postQuery(
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE metrics['country'] IS NULL");
    Assert.assertEquals(nullResponse.get("exceptions").size(), 0);
    long nullCount = nullResponse.get("resultTable").get("rows").get(0).get(0).longValue();
    Assert.assertEquals(nullCount, COUNTRY_NULL_COUNT);

    Assert.assertEquals(notNullCount + nullCount, TOTAL_DOCS,
        "IS_NOT_NULL + IS_NULL must equal total doc count");
  }

  /**
   * IS NOT NULL combined with aggregation: SUM should only aggregate non-null docs.
   */
  @Test
  public void testIsNotNullWithAggregation()
      throws Exception {
    JsonNode response = postQuery(
        "SELECT SUM(metrics['clicks']) FROM " + TABLE_NAME
            + " WHERE metrics['clicks'] IS NOT NULL");
    Assert.assertEquals(response.get("exceptions").size(), 0, "Query had exceptions: " + response);
    double sum = response.get("resultTable").get("rows").get(0).get(0).doubleValue();
    // The sum should be the same as the total clicks sum since absent keys contribute 0
    Assert.assertEquals(sum, CLICKS_SUM, 0.001);
  }
}
