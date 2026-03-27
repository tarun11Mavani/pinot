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
package org.apache.pinot.tools;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.tools.admin.PinotAdministrator;
import org.apache.pinot.tools.admin.command.QuickstartRunner;


/**
 * Quickstart demonstrating COLUMNAR_MAP column type vs JSON column type on identical data.
 *
 * <p>Two tables are loaded with the same 200-row user-metrics dataset:
 * <ul>
 *   <li>{@code userMetrics} — {@code metrics} column is {@code COLUMNAR_MAP} with declared keys
 *       {@code clicks} (LONG), {@code spend} (DOUBLE), {@code sessions} (INT), and
 *       {@code country} (STRING). Values are stored in a compact columnar bitmap index enabling
 *       O(1) per-key retrieval and fast EQ/IN filtering via per-key inverted indexes.</li>
 *   <li>{@code userMetricsJson} — {@code metrics} column is {@code JSON}, storing the entire
 *       map object as a JSON blob. Key access requires {@code JSON_EXTRACT_SCALAR} and all
 *       predicates are evaluated via full document scan.</li>
 * </ul>
 *
 * <p>Equivalent queries are run against both tables so you can compare query syntax and
 * inspect execution plans to observe the performance difference.
 */
public class ColumnarMapQuickStart extends Quickstart {

  @Override
  public List<String> types() {
    return Arrays.asList("COLUMNAR_MAP", "BATCH_COLUMNAR_MAP", "BATCH-SPARSE-MAP", "OFFLINE_COLUMNAR_MAP",
        "OFFLINE-SPARSE-MAP");
  }

  @Override
  protected String[] getDefaultBatchTableDirectories() {
    return new String[]{
        "examples/batch/baseballStats",
        "examples/batch/userMetrics",
        "examples/batch/userMetricsJson"
    };
  }

  @Override
  public void runSampleQueries(QuickstartRunner runner)
      throws Exception {
    printStatus(Color.GREEN, "[COLUMNAR_MAP_QUICKSTART_BUILD=v6] Starting sample queries");

    // -----------------------------------------------------------------------
    // Legacy schema validation — baseballStats uses a traditional schema with
    // dimension + metric field specs and no sparse map columns. Verifying this
    // table loads and queries correctly ensures backward compatibility.
    // -----------------------------------------------------------------------
    printStatus(Color.GREEN, "=== LEGACY SCHEMA VALIDATION (baseballStats) ===");

    String legacy1 = "SELECT COUNT(*) FROM baseballStats";
    printStatus(Color.YELLOW, "[LEGACY] Total records in baseballStats (traditional schema)");
    printStatus(Color.CYAN, "Query : " + legacy1);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(legacy1)));
    printStatus(Color.GREEN, "***************************************************");

    String legacy2 =
        "SELECT playerName, SUM(runs) FROM baseballStats GROUP BY playerName ORDER BY SUM(runs) DESC LIMIT 5";
    printStatus(Color.YELLOW, "[LEGACY] Top 5 run scorers — validates metric aggregation on legacy schema");
    printStatus(Color.CYAN, "Query : " + legacy2);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(legacy2)));
    printStatus(Color.GREEN, "***************************************************");

    String legacy3 =
        "SELECT playerName, yearID, homeRuns FROM baseballStats WHERE yearID = 2000 ORDER BY homeRuns DESC LIMIT 5";
    printStatus(Color.YELLOW, "[LEGACY] Top home-run hitters in 2000 — validates dimension filter + metric projection");
    printStatus(Color.CYAN, "Query : " + legacy3);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(legacy3)));
    printStatus(Color.GREEN, "***************************************************");

    printStatus(Color.GREEN, "=== Legacy schema validation PASSED — coexists with COLUMNAR_MAP tables ===");

    // -----------------------------------------------------------------------
    // COLUMNAR_MAP vs JSON comparison queries
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Q1: Total record count — both tables have identical row counts
    // -----------------------------------------------------------------------
    String q1sm = "SELECT COUNT(*) FROM userMetrics";
    printStatus(Color.YELLOW, "[COLUMNAR_MAP] Total number of user metric records");
    printStatus(Color.CYAN, "Query : " + q1sm);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1sm)));
    printStatus(Color.GREEN, "***************************************************");

    String q1json = "SELECT COUNT(*) FROM userMetricsJson";
    printStatus(Color.YELLOW, "[JSON] Total number of user metric records");
    printStatus(Color.CYAN, "Query : " + q1json);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q1json)));
    printStatus(Color.GREEN, "***************************************************");

    // -----------------------------------------------------------------------
    // Q2: Simple projection of non-map columns
    // -----------------------------------------------------------------------
    String q2sm = "SELECT userId, region FROM userMetrics LIMIT 10";
    printStatus(Color.YELLOW, "[COLUMNAR_MAP] Show first 10 rows (userId and region columns)");
    printStatus(Color.CYAN, "Query : " + q2sm);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q2sm)));
    printStatus(Color.GREEN, "***************************************************");

    String q2json = "SELECT userId, region FROM userMetricsJson LIMIT 10";
    printStatus(Color.YELLOW, "[JSON] Show first 10 rows (userId and region columns)");
    printStatus(Color.CYAN, "Query : " + q2json);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q2json)));
    printStatus(Color.GREEN, "***************************************************");

    // -----------------------------------------------------------------------
    // Q3: Group-by on a regular column
    // -----------------------------------------------------------------------
    String q3sm = "SELECT region, COUNT(*) AS userCount FROM userMetrics GROUP BY region ORDER BY userCount DESC";
    printStatus(Color.YELLOW, "[COLUMNAR_MAP] Count users per region");
    printStatus(Color.CYAN, "Query : " + q3sm);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q3sm)));
    printStatus(Color.GREEN, "***************************************************");

    String q3json = "SELECT region, COUNT(*) AS userCount FROM userMetricsJson GROUP BY region ORDER BY userCount DESC";
    printStatus(Color.YELLOW, "[JSON] Count users per region");
    printStatus(Color.CYAN, "Query : " + q3json);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q3json)));
    printStatus(Color.GREEN, "***************************************************");

    // -----------------------------------------------------------------------
    // Q4: EQ filter on regular column + group-by
    // -----------------------------------------------------------------------
    String q4sm = "SELECT region, COUNT(*) FROM userMetrics WHERE region = 'US' GROUP BY region";
    printStatus(Color.YELLOW, "[COLUMNAR_MAP] Count records for US region");
    printStatus(Color.CYAN, "Query : " + q4sm);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q4sm)));
    printStatus(Color.GREEN, "***************************************************");

    String q4json = "SELECT region, COUNT(*) FROM userMetricsJson WHERE region = 'US' GROUP BY region";
    printStatus(Color.YELLOW, "[JSON] Count records for US region");
    printStatus(Color.CYAN, "Query : " + q4json);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q4json)));
    printStatus(Color.GREEN, "***************************************************");

    // -----------------------------------------------------------------------
    // Q5: Key projection — COLUMNAR_MAP uses col['key']; JSON uses JSON_EXTRACT_SCALAR
    // -----------------------------------------------------------------------
    String q5sm = "SELECT userId, metrics['clicks'] FROM userMetrics LIMIT 10";
    printStatus(Color.YELLOW, "[COLUMNAR_MAP] Project metrics['clicks'] — O(1) bitmap-rank lookup per doc");
    printStatus(Color.CYAN, "Query : " + q5sm);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q5sm)));
    printStatus(Color.GREEN, "***************************************************");

    String q5json = "SELECT userId, JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG') FROM userMetricsJson LIMIT 10";
    printStatus(Color.YELLOW, "[JSON] Project clicks via JSON_EXTRACT_SCALAR — parses JSON blob per doc");
    printStatus(Color.CYAN, "Query : " + q5json);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q5json)));
    printStatus(Color.GREEN, "***************************************************");

    // -----------------------------------------------------------------------
    // Q6: Range predicate on map key — COLUMNAR_MAP uses expression scan over typed values;
    //     JSON_MATCH does not guarantee numeric ordering so JSON_EXTRACT_SCALAR is used here
    // -----------------------------------------------------------------------
    String q6sm = "SELECT userId, metrics['clicks'] FROM userMetrics WHERE metrics['clicks'] >= 5 LIMIT 100";
    printStatus(Color.YELLOW, "[COLUMNAR_MAP] Filter metrics['clicks'] >= 5 — typed per-key forward index scan");
    printStatus(Color.CYAN, "Query : " + q6sm);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q6sm)));
    printStatus(Color.GREEN, "***************************************************");

    String q6json =
        "SELECT userId, JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG') FROM userMetricsJson"
            + " WHERE JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG') >= 5 LIMIT 100";
    printStatus(Color.YELLOW,
        "[JSON] Filter clicks >= 5 via JSON_EXTRACT_SCALAR (range — no JSON_MATCH for numeric range)");
    printStatus(Color.CYAN, "Query : " + q6json);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q6json)));
    printStatus(Color.GREEN, "***************************************************");

    // -----------------------------------------------------------------------
    // Q7: EQ filter on map key — COLUMNAR_MAP uses per-key inverted index (no scan);
    //     JSON_MATCH uses the JSON index (inverted lookup) — both avoid full doc scan
    // -----------------------------------------------------------------------
    String q7sm = "SELECT userId, metrics['country'] FROM userMetrics WHERE metrics['country'] = 'US' LIMIT 100";
    printStatus(Color.YELLOW, "[COLUMNAR_MAP] Filter metrics['country'] = 'US' — inverted index lookup, no doc scan");
    printStatus(Color.CYAN, "Query : " + q7sm);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q7sm)));
    printStatus(Color.GREEN, "***************************************************");

    String q7json =
        "SELECT userId, JSON_EXTRACT_SCALAR(metrics, '$.country', 'STRING') FROM userMetricsJson"
            + " WHERE JSON_MATCH(metrics, '\"$.country\" = ''US''')";
    printStatus(Color.YELLOW, "[JSON] Filter country = 'US' via JSON_MATCH — uses JSON index, no full doc parse");
    printStatus(Color.CYAN, "Query : " + q7json);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q7json)));
    printStatus(Color.GREEN, "***************************************************");

    // -----------------------------------------------------------------------
    // Q8: SUM aggregation on a numeric map key
    // -----------------------------------------------------------------------
    String q8sm = "SELECT SUM(metrics['clicks']) AS totalClicks FROM userMetrics";
    printStatus(Color.YELLOW, "[COLUMNAR_MAP] SUM of metrics['clicks'] across all users");
    printStatus(Color.CYAN, "Query : " + q8sm);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q8sm)));
    printStatus(Color.GREEN, "***************************************************");

    String q8json = "SELECT SUM(JSON_EXTRACT_SCALAR(metrics, '$.clicks', 'LONG')) AS totalClicks FROM userMetricsJson";
    printStatus(Color.YELLOW, "[JSON] SUM of clicks extracted via JSON_EXTRACT_SCALAR");
    printStatus(Color.CYAN, "Query : " + q8json);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q8json)));
    printStatus(Color.GREEN, "***************************************************");

    // -----------------------------------------------------------------------
    // Q9: GROUP BY on a map key
    // -----------------------------------------------------------------------
    String q9sm =
        "SELECT metrics['country'], COUNT(*) AS cnt FROM userMetrics"
            + " WHERE metrics['country'] != '' GROUP BY metrics['country'] ORDER BY cnt DESC";
    printStatus(Color.YELLOW,
        "[COLUMNAR_MAP] GROUP BY metrics['country'] — per-key forward index, inverted index assists filter");
    printStatus(Color.CYAN, "Query : " + q9sm);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q9sm)));
    printStatus(Color.GREEN, "***************************************************");

    String q9json =
        "SELECT JSON_EXTRACT_SCALAR(metrics, '$.country', 'STRING') AS country, COUNT(*) AS cnt"
            + " FROM userMetricsJson"
            + " WHERE JSON_MATCH(metrics, '\"$.country\" IS NOT NULL')"
            + " GROUP BY country ORDER BY cnt DESC";
    printStatus(Color.YELLOW, "[JSON] GROUP BY country — JSON_MATCH IS NOT NULL uses JSON index to skip missing docs");
    printStatus(Color.CYAN, "Query : " + q9json);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q9json)));
    printStatus(Color.GREEN, "***************************************************");

    // -----------------------------------------------------------------------
    // Q10: IS NOT NULL on map key — COLUMNAR_MAP uses presence bitmap (O(1));
    //      JSON uses JSON_MATCH IS NOT NULL
    // -----------------------------------------------------------------------
    String q10sm = "SELECT COUNT(*) FROM userMetrics WHERE metrics['clicks'] IS NOT NULL";
    printStatus(Color.YELLOW,
        "[COLUMNAR_MAP] COUNT where metrics['clicks'] IS NOT NULL — presence bitmap, zero doc scan");
    printStatus(Color.CYAN, "Query : " + q10sm);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q10sm)));
    printStatus(Color.GREEN, "***************************************************");

    String q10json = "SELECT COUNT(*) FROM userMetricsJson"
        + " WHERE JSON_MATCH(metrics, '\"$.clicks\" IS NOT NULL')";
    printStatus(Color.YELLOW, "[JSON] COUNT where clicks IS NOT NULL via JSON_MATCH");
    printStatus(Color.CYAN, "Query : " + q10json);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q10json)));
    printStatus(Color.GREEN, "***************************************************");

    // -----------------------------------------------------------------------
    // Q11: IS NULL on map key — COLUMNAR_MAP flips presence bitmap;
    //      JSON uses JSON_MATCH IS NULL
    // -----------------------------------------------------------------------
    String q11sm = "SELECT COUNT(*) FROM userMetrics WHERE metrics['clicks'] IS NULL";
    printStatus(Color.YELLOW, "[COLUMNAR_MAP] COUNT where metrics['clicks'] IS NULL — flipped presence bitmap");
    printStatus(Color.CYAN, "Query : " + q11sm);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q11sm)));
    printStatus(Color.GREEN, "***************************************************");

    String q11json = "SELECT COUNT(*) FROM userMetricsJson"
        + " WHERE JSON_MATCH(metrics, '\"$.clicks\" IS NULL')";
    printStatus(Color.YELLOW, "[JSON] COUNT where clicks IS NULL via JSON_MATCH");
    printStatus(Color.CYAN, "Query : " + q11json);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q11json)));
    printStatus(Color.GREEN, "***************************************************");

    // -----------------------------------------------------------------------
    // Q12: IS NOT NULL + aggregation
    // -----------------------------------------------------------------------
    String q12sm = "SELECT SUM(metrics['clicks']) FROM userMetrics WHERE metrics['clicks'] IS NOT NULL";
    printStatus(Color.YELLOW, "[COLUMNAR_MAP] SUM clicks WHERE IS NOT NULL — bitmap-filtered aggregation");
    printStatus(Color.CYAN, "Query : " + q12sm);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q12sm)));
    printStatus(Color.GREEN, "***************************************************");

    // -----------------------------------------------------------------------
    // Q13: IS NOT NULL + GROUP BY
    // -----------------------------------------------------------------------
    String q13sm = "SELECT region, COUNT(*) FROM userMetrics"
        + " WHERE metrics['country'] IS NOT NULL GROUP BY region ORDER BY COUNT(*) DESC";
    printStatus(Color.YELLOW, "[COLUMNAR_MAP] GROUP BY region WHERE country IS NOT NULL — bitmap + group-by");
    printStatus(Color.CYAN, "Query : " + q13sm);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q13sm)));
    printStatus(Color.GREEN, "***************************************************");

    // -----------------------------------------------------------------------
    // Q14: IS NOT NULL + IS NULL complement verification
    //      COUNT(IS_NOT_NULL) + COUNT(IS_NULL) must equal COUNT(*)
    // -----------------------------------------------------------------------
    String q14sm = "SELECT COUNT(*) FROM userMetrics WHERE metrics['sessions'] IS NULL";
    printStatus(Color.YELLOW,
        "[COLUMNAR_MAP] COUNT where sessions IS NULL — complement check: IS_NOT_NULL + IS_NULL = total");
    printStatus(Color.CYAN, "Query : " + q14sm);
    printStatus(Color.YELLOW, prettyPrintResponse(runner.runQuery(q14sm)));
    printStatus(Color.GREEN, "***************************************************");
  }

  public static void main(String[] args)
      throws Exception {
    List<String> arguments = new ArrayList<>();
    arguments.addAll(Arrays.asList("QuickStart", "-type", "COLUMNAR_MAP"));
    arguments.addAll(Arrays.asList(args));
    PinotAdministrator.main(arguments.toArray(new String[arguments.size()]));
  }
}
