/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.sap.transformer;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.sap.odata.ODataEntity;
import org.apache.olingo.odata2.api.edm.EdmLiteralKind;
import org.apache.olingo.odata2.api.edm.EdmSimpleTypeException;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.apache.olingo.odata2.core.edm.EdmDateTimeOffset;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Transforms {@link ODataEntity} to {@link StructuredRecord}.
 */
public class ODataEntryToRecordTransformer {

  private final Schema schema;

  public ODataEntryToRecordTransformer(Schema schema) {
    this.schema = schema;
  }

  /**
   * Transforms given {@link ODataEntry} to {@link StructuredRecord}.
   *
   * @param oDataEntity ODataEntity to be transformed.
   * @return {@link StructuredRecord} that corresponds to the given {@link ODataEntity}.
   */
  public StructuredRecord transform(ODataEntity oDataEntity) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      Schema nonNullableSchema = field.getSchema().isNullable() ?
        field.getSchema().getNonNullable() : field.getSchema();
      String fieldName = field.getName();
      Object value = oDataEntity.getProperties().get(fieldName);
      builder.set(fieldName, extractValue(fieldName, value, nonNullableSchema));
    }
    return builder.build();
  }

  /**
   * Extract value of EDM types according to the provided schema. Some of the EDM types can be represented by multiple
   * Java types. For more information see:
   * <a href="https://olingo.apache.org/javadoc/odata2/org/apache/olingo/odata2/api/edm/EdmSimpleType.html">
   * EdmSimpleType
   * </a>
   */
  private Object extractValue(String fieldName, Object value, Schema schema) {
    if (value == null) {
      return null;
    }

    Schema.LogicalType fieldLogicalType = schema.getLogicalType();
    if (fieldLogicalType != null) {
      switch (fieldLogicalType) {
        case TIMESTAMP_MILLIS:
          ensureTypeValid(fieldName, value, Calendar.class, Timestamp.class);
          return extractTimestampMillis(value);
        case TIMESTAMP_MICROS:
          ensureTypeValid(fieldName, value, Calendar.class, Timestamp.class);
          return extractTimestampMicros(value);
        case TIME_MILLIS:
          ensureTypeValid(fieldName, value, GregorianCalendar.class, Timestamp.class);
          return extractTimeMillis(value);
        case TIME_MICROS:
          ensureTypeValid(fieldName, value, GregorianCalendar.class, Timestamp.class);
          return extractTimeMicros(value);
        case DECIMAL:
          ensureTypeValid(fieldName, value, BigDecimal.class, BigInteger.class, Double.class, Float.class, Byte.class,
                          Short.class, Integer.class, Long.class);
          return extractDecimal(fieldName, value, schema);
        default:
          throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'", fieldName,
                                                            fieldLogicalType.getToken()));
      }
    }

    Schema.Type fieldType = schema.getType();
    switch (fieldType) {
      case BOOLEAN:
        ensureTypeValid(fieldName, value, Boolean.class);
        return value;
      case INT:
        ensureTypeValid(fieldName, value, Short.class, Byte.class, Integer.class, Long.class, BigInteger.class);
        return ((Number) value).intValue();
      case FLOAT:
        ensureTypeValid(fieldName, value, Float.class, Double.class, BigDecimal.class, Byte.class, Short.class,
                        Integer.class, Long.class);
        return ((Number) value).floatValue();
      case DOUBLE:
        ensureTypeValid(fieldName, value, Double.class, Float.class, BigDecimal.class, Byte.class, Short.class,
                        Integer.class, Long.class);
        return ((Number) value).doubleValue();
      case BYTES:
        ensureTypeValid(fieldName, value, byte[].class);
        return value;
      case LONG:
        ensureTypeValid(fieldName, value, Long.class, Byte.class, Short.class, Integer.class, BigInteger.class);
        return ((Number) value).longValue();
      case STRING:
        ensureTypeValid(fieldName, value, String.class, UUID.class, Calendar.class, Timestamp.class, BigDecimal.class);
        if (value instanceof Calendar || value instanceof Timestamp) {
          // Olingo V4 uses Timestamp for 'Edm.DateTimeOffset'
          return extractDateTimeOffset(fieldName, value);
        }
        return value.toString();
      default:
        throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'", fieldName,
                                                          fieldType.name().toLowerCase()));
    }
  }

  private String extractDateTimeOffset(String fieldName, Object value) {
    try {
      return EdmDateTimeOffset.getInstance().valueToString(value, EdmLiteralKind.DEFAULT, null);
    } catch (EdmSimpleTypeException e) {
      throw new UnexpectedFormatException(String.format("Unsupported value for '%s' field: '%s'", fieldName, value), e);
    }
  }

  private int extractTimeMillis(Object value) {
    long nanos = value instanceof GregorianCalendar
      ? ((GregorianCalendar) value).toZonedDateTime().toLocalTime().toNanoOfDay()
      : ((Timestamp) value).toLocalDateTime().toLocalTime().toNanoOfDay();
    return Math.toIntExact(TimeUnit.NANOSECONDS.toMillis(nanos));
  }

  private long extractTimeMicros(Object value) {
    long nanos = value instanceof GregorianCalendar
      ? ((GregorianCalendar) value).toZonedDateTime().toLocalTime().toNanoOfDay()
      : ((Timestamp) value).toLocalDateTime().toLocalTime().toNanoOfDay();
    return TimeUnit.NANOSECONDS.toMicros(nanos);
  }

  private long extractTimestampMillis(Object value) {
    Instant instant = value instanceof Calendar ? ((Calendar) value).toInstant() : ((Timestamp) value).toInstant();
    long millis = TimeUnit.SECONDS.toMillis(instant.getEpochSecond());
    return Math.addExact(millis, TimeUnit.NANOSECONDS.toMillis(instant.getNano()));
  }

  private long extractTimestampMicros(Object value) {
    Instant instant = value instanceof Calendar ? ((Calendar) value).toInstant() : ((Timestamp) value).toInstant();
    long micros = TimeUnit.SECONDS.toMicros(instant.getEpochSecond());
    return Math.addExact(micros, TimeUnit.NANOSECONDS.toMicros(instant.getNano()));
  }

  private byte[] extractDecimal(String fieldName, Object value, Schema schema) {
    int schemaPrecision = schema.getPrecision();
    int schemaScale = schema.getScale();
    BigDecimal decimal = extractBigDecimal(value, schema);
    if (decimal.precision() > schemaPrecision) {
      throw new UnexpectedFormatException(
        String.format("Field '%s' has precision '%s' which is higher than schema precision '%s'.",
                      fieldName, decimal.precision(), schemaPrecision));
    }

    if (decimal.scale() > schemaScale) {
      throw new UnexpectedFormatException(
        String.format("Field '%s' has scale '%s' which is not equal to schema scale '%s'.",
                      fieldName, decimal.scale(), schemaScale));
    }

    return decimal.setScale(schemaScale).unscaledValue().toByteArray();
  }

  /**
   * Extracts {@link BigDecimal} value of 'EDM.Decimal' since EDM.Decimal can be represented by multiple Java types
   * in Olingo V4: {@link BigDecimal}, {@link BigInteger}, {@link Double}, {@link Float}, {@link Byte}, {@link Short},
   * {@link Integer}, {@link Long}.
   *
   * For more information see:
   * <a href="https://olingo.apache.org/javadoc/odata4/org/apache/olingo/commons/api/edm/EdmPrimitiveType.html">
   *   EdmPrimitiveType
   * </a>
   *
   * @param value 'EDM.Decimal' value of one of the following Java types {@link BigDecimal}, {@link BigInteger},
   * {@link Double}, {@link Float}, {@link Byte}, {@link Short}, {@link Integer}, {@link Long}.
   * @param schema field schema.
   * @return {@link BigDecimal} representation of the provided 'EDM.Decimal' value.
   */
  private BigDecimal extractBigDecimal(Object value, Schema schema) {
    if (value instanceof BigDecimal) {
      return (BigDecimal) value;
    }
    if (value instanceof BigInteger) {
      return new BigDecimal((BigInteger) value);
    }
    if (value instanceof Double || value instanceof Float) {
      double doubleValue = ((Number) value).doubleValue();
      int precision = schema.getPrecision();
      int scale = schema.getScale();
      return new BigDecimal(doubleValue, new MathContext(precision)).setScale(scale, BigDecimal.ROUND_HALF_EVEN);
    }

    // Byte, Short, Integer, Long
    long longValue = ((Number) value).longValue();
    return new BigDecimal(longValue);
  }

  private void ensureTypeValid(String fieldName, Object value, Class... expectedTypes) {
    for (Class expectedType : expectedTypes) {
      if (expectedType.isInstance(value)) {
        return;
      }
    }

    String expectedTypeNames = Stream.of(expectedTypes)
      .map(Class::getName)
      .collect(Collectors.joining(", "));
    throw new UnexpectedFormatException(
      String.format("SAP field '%s' is expected to be of type '%s', but found a '%s'.", fieldName,
                    expectedTypeNames, value.getClass().getSimpleName()));
  }
}
