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
package org.apache.pinot.plugin.inputformat.avro;

import com.google.common.collect.Maps;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.readers.BaseRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;


/**
 * Extractor for Avro Records
 */
public class AvroRecordExtractor extends BaseRecordExtractor<GenericRecord> {
  private Set<String> _fields;
  private boolean _extractAll = false;
  private boolean _applyLogicalTypes = true;

  @Override
  public void init(@Nullable Set<String> fields, @Nullable RecordExtractorConfig recordExtractorConfig) {
    AvroRecordExtractorConfig config = (AvroRecordExtractorConfig) recordExtractorConfig;
    if (config != null) {
      _applyLogicalTypes = config.isEnableLogicalTypes();
    }
    if (fields == null || fields.isEmpty()) {
      _extractAll = true;
      _fields = Set.of();
    } else {
      _fields = Set.copyOf(fields);
    }
  }

  @Override
  public GenericRow extract(GenericRecord from, GenericRow to) {
    if (_extractAll) {
      List<Schema.Field> fields = from.getSchema().getFields();
      for (Schema.Field field : fields) {
        String fieldName = field.name();
        Object value = from.get(fieldName);
        if (_applyLogicalTypes) {
          value = AvroSchemaUtil.applyLogicalType(field, value);
        }
        if (value != null) {
          value = transformValue(value, field);
        }
        to.putValue(fieldName, value);
      }
    } else {
      for (String fieldName : _fields) {
        Schema.Field field = from.getSchema().getField(fieldName);
        Object value = field == null ? null : from.get(field.pos());
        if (_applyLogicalTypes) {
          value = AvroSchemaUtil.applyLogicalType(field, value);
        }
        if (value != null) {
          value = transformValue(value, field);
        }
        to.putValue(fieldName, value);
      }
    }
    return to;
  }

  protected Object transformValue(Object value, Schema.Field field) {
    return convert(value);
  }

  /**
   * Returns whether the object is an Avro GenericRecord.
   */
  @Override
  protected boolean isRecord(Object value) {
    return value instanceof GenericRecord;
  }

  /**
   * Handles the conversion of every field of the Avro GenericRecord.
   *
   * @param value should be verified to be a GenericRecord type prior to calling this method as it will be casted
   *              without checking
   */
  @Override
  protected Map<Object, Object> convertRecord(Object value) {
    GenericRecord record = (GenericRecord) value;
    List<Schema.Field> fields = record.getSchema().getFields();
    Map<Object, Object> convertedMap = Maps.newHashMapWithExpectedSize(fields.size());
    for (Schema.Field field : fields) {
      String fieldName = field.name();
      Object fieldValue = record.get(fieldName);
      Object convertedValue = fieldValue != null ? transformValue(fieldValue, field) : null;
      convertedMap.put(fieldName, convertedValue);
    }
    return convertedMap;
  }

  /**
   * This method convert any Avro logical-type converted (or not) value to a class supported by
   * Pinot {@link GenericRow}
   *
   * Note that at the moment BigDecimal is converted to Pinot double which may lead to precision loss or may not be
   * represented at all.
   * Similarly, timestamp microsecond precision is not supported at the moment. These values will get converted to
   * millisecond precision.
   */
  @Override
  protected Object convertSingleValue(Object value) {
    if (value instanceof Instant) {
      return Timestamp.from((Instant) value);
    }
    if (value instanceof GenericFixed) {
      return ((GenericFixed) value).bytes();
    }
    // LocalDate, LocalTime and UUID are returned as the ::toString version of the logical type
    return super.convertSingleValue(value);
  }
}
