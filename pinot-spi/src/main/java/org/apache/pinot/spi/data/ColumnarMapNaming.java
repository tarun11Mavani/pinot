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


/// Naming convention for COLUMNAR_MAP materialized columns. Each dense MAP key is stored as
/// a column named `<mapColumn>$__<key>`. Sparse keys share a single synthetic JSON column
/// named `<mapColumn>$____sparse__`.
public final class ColumnarMapNaming {
  public static final String SEPARATOR = "$__";
  public static final String SPARSE_SUFFIX = "__sparse__";

  private ColumnarMapNaming() {
  }

  public static String materializedColumnName(String mapColumn, String key) {
    return mapColumn + SEPARATOR + key;
  }

  public static String sparseColumnName(String mapColumn) {
    return mapColumn + SEPARATOR + SPARSE_SUFFIX;
  }

  public static boolean isMaterializedMapColumn(String columnName) {
    return columnName.contains(SEPARATOR);
  }

  public static boolean isSparseColumn(String columnName) {
    return columnName.endsWith(SEPARATOR + SPARSE_SUFFIX);
  }

  public static String parseMapColumn(String materializedColumnName) {
    int idx = materializedColumnName.indexOf(SEPARATOR);
    return idx >= 0 ? materializedColumnName.substring(0, idx) : materializedColumnName;
  }

  public static String parseKey(String materializedColumnName) {
    int idx = materializedColumnName.indexOf(SEPARATOR);
    return idx >= 0 ? materializedColumnName.substring(idx + SEPARATOR.length()) : materializedColumnName;
  }
}
