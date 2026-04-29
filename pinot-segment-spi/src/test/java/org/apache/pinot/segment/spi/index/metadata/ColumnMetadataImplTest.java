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
package org.apache.pinot.segment.spi.index.metadata;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class ColumnMetadataImplTest {

  @Test
  public void fallsBackToDictionaryEncodingWhenKeyAbsentAndHasDictionary() {
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.HAS_DICTIONARY), true);

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertTrue(metadata.hasDictionary());
    assertEquals(metadata.getForwardIndexEncoding(), EncodingType.DICTIONARY,
        "Old segments without FORWARD_INDEX_ENCODING and HAS_DICTIONARY=true must infer DICTIONARY encoding");
  }

  @Test
  public void fallsBackToRawEncodingWhenKeyAbsentAndNoDictionary() {
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.HAS_DICTIONARY), false);

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertFalse(metadata.hasDictionary());
    assertEquals(metadata.getForwardIndexEncoding(), EncodingType.RAW,
        "Old segments without FORWARD_INDEX_ENCODING and HAS_DICTIONARY=false must infer RAW encoding");
  }

  @Test
  public void honorsExplicitRawEncodingEvenWhenHasDictionary() {
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.HAS_DICTIONARY), true);
    config.setProperty(Column.getKeyFor("col", Column.FORWARD_INDEX_ENCODING), EncodingType.RAW.name());

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertTrue(metadata.hasDictionary());
    assertEquals(metadata.getForwardIndexEncoding(), EncodingType.RAW,
        "Explicit FORWARD_INDEX_ENCODING=RAW must override inference even when HAS_DICTIONARY=true (shared-dict)");
  }

  @Test
  public void honorsExplicitDictionaryEncoding() {
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.HAS_DICTIONARY), true);
    config.setProperty(Column.getKeyFor("col", Column.FORWARD_INDEX_ENCODING), EncodingType.DICTIONARY.name());

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertTrue(metadata.hasDictionary());
    assertEquals(metadata.getForwardIndexEncoding(), EncodingType.DICTIONARY);
  }

  @Test
  public void testVirtualColumnMetadataFromProperties() {
    PropertiesConfiguration props = new PropertiesConfiguration();
    String column = "metrics$__tenancy";
    props.setProperty("column." + column + ".dataType", "STRING");
    props.setProperty("column." + column + ".columnType", "DIMENSION");
    props.setProperty("column." + column + ".isSingleValues", "true");
    props.setProperty("column." + column + ".cardinality", "47");
    props.setProperty("column." + column + ".hasDictionary", "true");
    props.setProperty("column." + column + ".virtualColumn", "true");
    props.setProperty("column." + column + ".parentMapColumn", "metrics");

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(props, 5000000, column);
    assertTrue(metadata.isVirtualColumn());
    assertEquals(metadata.getParentMapColumn(), "metrics");
    assertEquals(metadata.getCardinality(), 47);
    assertEquals(metadata.getTotalDocs(), 5000000);
  }

  @Test
  public void testNonVirtualColumnDefaultValues() {
    PropertiesConfiguration props = new PropertiesConfiguration();
    String column = "normalCol";
    props.setProperty("column." + column + ".dataType", "INT");
    props.setProperty("column." + column + ".columnType", "DIMENSION");
    props.setProperty("column." + column + ".isSingleValues", "true");
    props.setProperty("column." + column + ".cardinality", "10");

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(props, 1000, column);
    assertFalse(metadata.isVirtualColumn());
    assertNull(metadata.getParentMapColumn());
  }

  @Test
  public void testVirtualColumnViaBuilder() {
    ColumnMetadataImpl metadata = ColumnMetadataImpl.builder()
        .setFieldSpec(new DimensionFieldSpec("metrics$__tenancy", FieldSpec.DataType.STRING, true))
        .setTotalDocs(100)
        .setCardinality(5)
        .setVirtualColumn(true)
        .setParentMapColumn("metrics")
        .build();

    assertTrue(metadata.isVirtualColumn());
    assertEquals(metadata.getParentMapColumn(), "metrics");
  }

  @Test
  public void testVirtualColumnIncludedInEqualsAndHashCode() {
    ColumnMetadataImpl m1 = ColumnMetadataImpl.builder()
        .setFieldSpec(new DimensionFieldSpec("col", FieldSpec.DataType.STRING, true))
        .setTotalDocs(100)
        .setCardinality(5)
        .setVirtualColumn(true)
        .setParentMapColumn("parent")
        .build();

    ColumnMetadataImpl m2 = ColumnMetadataImpl.builder()
        .setFieldSpec(new DimensionFieldSpec("col", FieldSpec.DataType.STRING, true))
        .setTotalDocs(100)
        .setCardinality(5)
        .setVirtualColumn(false)
        .build();

    assertNotEquals(m1, m2);

    ColumnMetadataImpl m3 = ColumnMetadataImpl.builder()
        .setFieldSpec(new DimensionFieldSpec("col", FieldSpec.DataType.STRING, true))
        .setTotalDocs(100)
        .setCardinality(5)
        .setVirtualColumn(true)
        .setParentMapColumn("parent")
        .build();

    assertEquals(m1, m3);
    assertEquals(m1.hashCode(), m3.hashCode());
  }

  private static PropertiesConfiguration baseConfig(String column) {
    PropertiesConfiguration config = new PropertiesConfiguration();
    config.setProperty(Column.getKeyFor(column, Column.COLUMN_NAME), column);
    config.setProperty(Column.getKeyFor(column, Column.COLUMN_TYPE), FieldType.DIMENSION.name());
    config.setProperty(Column.getKeyFor(column, Column.DATA_TYPE), DataType.STRING.name());
    config.setProperty(Column.getKeyFor(column, Column.IS_SINGLE_VALUED), true);
    config.setProperty(Column.getKeyFor(column, Column.CARDINALITY), 1);
    return config;
  }
}
