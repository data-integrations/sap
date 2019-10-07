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
import io.cdap.plugin.sap.SapODataConstants;
import io.cdap.plugin.sap.odata.ODataEntity;
import io.cdap.plugin.sap.odata.StreamProperty;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.geo.ComposedGeospatial;
import org.apache.olingo.commons.api.edm.geo.Geospatial;
import org.apache.olingo.commons.api.edm.geo.GeospatialCollection;
import org.apache.olingo.commons.api.edm.geo.LineString;
import org.apache.olingo.commons.api.edm.geo.MultiLineString;
import org.apache.olingo.commons.api.edm.geo.MultiPoint;
import org.apache.olingo.commons.api.edm.geo.MultiPolygon;
import org.apache.olingo.commons.api.edm.geo.Point;
import org.apache.olingo.commons.api.edm.geo.Polygon;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDuration;
import org.apache.olingo.odata2.api.edm.EdmLiteralKind;
import org.apache.olingo.odata2.api.edm.EdmSimpleTypeException;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;
import org.apache.olingo.odata2.core.edm.EdmDateTimeOffset;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Transforms {@link ODataEntity} to {@link StructuredRecord}.
 */
public class ODataEntryToRecordTransformer {

  protected final Schema schema;

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
  protected Object extractValue(String fieldName, Object value, Schema schema) {
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
        if (value instanceof BigDecimal) {
          return extractDuration(fieldName, (BigDecimal) value);
        }
        return value.toString();
      case RECORD:
        ensureTypeValid(fieldName, value, Geospatial.class, StreamProperty.class, Map.class);
        if (value instanceof StreamProperty) {
          return extractStream((StreamProperty) value);
        }
        if (value instanceof Map) {
          return extractComplexValue((Map<String, Object>) value, schema);
        }
        return extractGeospatial(fieldName, (Geospatial) value);
      case ARRAY:
        ensureTypeValid(fieldName, value, List.class);
        Schema componentSchema = schema.getComponentSchema().isNullable() ? schema.getComponentSchema().getNonNullable()
          : schema.getComponentSchema();
        return extractCollection(fieldName, (List<Object>) value, componentSchema);
      default:
        throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported type '%s'", fieldName,
                                                          fieldType.name().toLowerCase()));
    }
  }

  private StructuredRecord extractStream(StreamProperty streamProperty) {
    return StructuredRecord.builder(SapODataConstants.Stream.SCHEMA)
      .set(SapODataConstants.Stream.ETAG_FIELD_NAME, streamProperty.getMediaEtag())
      .set(SapODataConstants.Stream.CONTENT_TYPE_FIELD_NAME, streamProperty.getMediaContentType())
      .set(SapODataConstants.Stream.READ_LINK_FIELD_NAME, streamProperty.getMediaReadLink())
      .set(SapODataConstants.Stream.EDIT_LINK_FIELD_NAME, streamProperty.getMediaEditLink())
      .build();
  }

  private List<Object> extractCollection(String fieldName, List<Object> collection, Schema componentSchema) {
    return collection.stream()
      .map(item -> extractValue(fieldName, item, componentSchema))
      .collect(Collectors.toList());
  }

  private StructuredRecord extractComplexValue(Map<String, Object> complexValue, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    schema.getFields().forEach(field -> {
      String fieldName = field.getName();
      Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
      Object extractedValue = extractValue(fieldName, complexValue.get(fieldName), fieldSchema);
      builder.set(fieldName, extractedValue);
    });

    return builder.build();
  }

  private StructuredRecord extractGeospatial(String fieldName, Geospatial geospatial) {
    Geospatial.Type geoType = geospatial.getGeoType();
    switch (geoType) {
      case POINT:
        return extractPoint((Point) geospatial);
      case LINESTRING:
        return extractLineString((LineString) geospatial);
      case POLYGON:
        return extractPolygon((Polygon) geospatial);
      case MULTIPOINT:
        return extractMultiPoint((MultiPoint) geospatial);
      case MULTILINESTRING:
        return extractMultiLineString((MultiLineString) geospatial);
      case MULTIPOLYGON:
        return extractMultiPolygon((MultiPolygon) geospatial);
      case GEOSPATIALCOLLECTION:
        return extractGeospatialCollectionRecord(fieldName, (GeospatialCollection) geospatial);
      default:
        // this should never happen
        throw new UnexpectedFormatException(String.format("Field '%s' is of unsupported geospatial type '%s'.",
                                                          fieldName, geoType));
    }
  }

  private StructuredRecord extractGeospatialCollectionRecord(String fieldName, GeospatialCollection collection) {
    List<StructuredRecord> points = new ArrayList<>();
    List<StructuredRecord> lineStrings = new ArrayList<>();
    List<StructuredRecord> polygons = new ArrayList<>();
    List<StructuredRecord> multiPoints = new ArrayList<>();
    List<StructuredRecord> multiLineStrings = new ArrayList<>();
    List<StructuredRecord> multiPolygons = new ArrayList<>();
    collection.iterator().forEachRemaining(g -> {
      switch (g.getGeoType()) {
        case POINT:
          points.add(extractGeospatial(fieldName, g));
          break;
        case LINESTRING:
          lineStrings.add(extractGeospatial(fieldName, g));
          break;
        case POLYGON:
          polygons.add(extractGeospatial(fieldName, g));
          break;
        case MULTIPOINT:
          multiPoints.add(extractGeospatial(fieldName, g));
          break;
        case MULTILINESTRING:
          multiLineStrings.add(extractGeospatial(fieldName, g));
          break;
        case MULTIPOLYGON:
          multiPolygons.add(extractGeospatial(fieldName, g));
          break;
      }
    });

    return StructuredRecord.builder(SapODataConstants.GeospatialCollection.SCHEMA)
      .set(SapODataConstants.GeospatialCollection.TYPE_FIELD_NAME, "GeometryCollection")
      .set(SapODataConstants.GeospatialCollection.DIMENSION_FIELD_NAME, collection.getDimension().name())
      .set(SapODataConstants.GeospatialCollection.POINTS_FIELD_NAME, points)
      .set(SapODataConstants.GeospatialCollection.LINE_STRINGS_FIELD_NAME, lineStrings)
      .set(SapODataConstants.GeospatialCollection.POLYGONS_FIELD_NAME, polygons)
      .set(SapODataConstants.GeospatialCollection.MULTI_POINTS_FIELD_NAME, multiPoints)
      .set(SapODataConstants.GeospatialCollection.MULTI_LINE_STRINGS_FIELD_NAME, multiLineStrings)
      .set(SapODataConstants.GeospatialCollection.MULTI_POLYGONS_FIELD_NAME, multiPolygons)
      .build();
  }

  private StructuredRecord extractPoint(Point point) {
    return StructuredRecord.builder(SapODataConstants.Point.SCHEMA)
      .set(SapODataConstants.Point.DIMENSION_FIELD_NAME, point.getDimension().name())
      .set(SapODataConstants.Point.X_FIELD_NAME, point.getX())
      .set(SapODataConstants.Point.Y_FIELD_NAME, point.getY())
      .set(SapODataConstants.Point.Z_FIELD_NAME, point.getZ())
      .build();
  }

  private StructuredRecord extractLineString(ComposedGeospatial<Point> lineString) {
    List<StructuredRecord> coordinates = new ArrayList<>();
    Iterator<Point> pointIterator = lineString.iterator();
    if (pointIterator != null) {
      pointIterator.forEachRemaining(p -> coordinates.add(extractPoint(p)));
    }
    // "LineString" and "MultiPoint" schemas are the same. Type name is required to distinguish them
    return StructuredRecord.builder(SapODataConstants.LineString.SCHEMA)
      .set(SapODataConstants.LineString.TYPE_FIELD_NAME, "LineString")
      .set(SapODataConstants.LineString.DIMENSION_FIELD_NAME, lineString.getDimension().name())
      .set(SapODataConstants.LineString.COORDINATES_FIELD_NAME, coordinates)
      .build();
  }

  private StructuredRecord extractPolygon(Polygon polygon) {
    List<StructuredRecord> exterior = new ArrayList<>();
    if (polygon.getExterior() != null && polygon.getExterior().iterator() != null) {
      polygon.getExterior().iterator().forEachRemaining(point -> exterior.add(extractPoint(point)));
    }
    List<StructuredRecord> interior = new ArrayList<>();
    for (int i = 0; i < polygon.getNumberOfInteriorRings(); i++) {
      interior.add(extractLineString(polygon.getInterior(i)));
    }

    return StructuredRecord.builder(SapODataConstants.Polygon.SCHEMA)
      .set(SapODataConstants.Polygon.TYPE_FIELD_NAME, "Polygon")
      .set(SapODataConstants.Polygon.DIMENSION_FIELD_NAME, polygon.getDimension().name())
      .set(SapODataConstants.Polygon.EXTERIOR_FIELD_NAME, exterior)
      .set(SapODataConstants.Polygon.INTERIOR_FIELD_NAME, interior)
      .set(SapODataConstants.Polygon.NUMBER_OF_INTERIOR_RINGS_FIELD_NAME, polygon.getNumberOfInteriorRings())
      .build();
  }

  private StructuredRecord extractMultiPoint(MultiPoint multiPoint) {
    List<StructuredRecord> coordinates = new ArrayList<>();
    Iterator<Point> pointIterator = multiPoint.iterator();
    if (pointIterator != null) {
      pointIterator.forEachRemaining(p -> coordinates.add(extractPoint(p)));
    }
    // "LineString" and "MultiPoint" schemas are the same. Type name is required to distinguish them
    return StructuredRecord.builder(SapODataConstants.MultiPoint.SCHEMA)
      .set(SapODataConstants.MultiPoint.TYPE_FIELD_NAME, "MultiPoint")
      .set(SapODataConstants.MultiPoint.DIMENSION_FIELD_NAME, multiPoint.getDimension().name())
      .set(SapODataConstants.MultiPoint.COORDINATES_FIELD_NAME, coordinates)
      .build();
  }

  private StructuredRecord extractMultiLineString(MultiLineString multiLineString) {
    List<StructuredRecord> coordinates = new ArrayList<>();
    Iterator<LineString> lineStringIterator = multiLineString.iterator();
    if (lineStringIterator != null) {
      lineStringIterator.forEachRemaining(ls -> coordinates.add(extractLineString(ls)));
    }

    return StructuredRecord.builder(SapODataConstants.MultiLineString.SCHEMA)
      .set(SapODataConstants.MultiLineString.TYPE_FIELD_NAME, "MultiLineString")
      .set(SapODataConstants.MultiLineString.DIMENSION_FIELD_NAME, multiLineString.getDimension().name())
      .set(SapODataConstants.MultiLineString.COORDINATES_FIELD_NAME, coordinates)
      .build();
  }

  private StructuredRecord extractMultiPolygon(MultiPolygon multiPolygon) {
    List<StructuredRecord> coordinates = new ArrayList<>();
    Iterator<Polygon> polygonIterator = multiPolygon.iterator();
    if (polygonIterator != null) {
      polygonIterator.forEachRemaining(polygon -> coordinates.add(extractPolygon(polygon)));
    }

    return StructuredRecord.builder(SapODataConstants.MultiPolygon.SCHEMA)
      .set(SapODataConstants.MultiPolygon.TYPE_FIELD_NAME, "MultiPolygon")
      .set(SapODataConstants.MultiPolygon.DIMENSION_FIELD_NAME, multiPolygon.getDimension().name())
      .set(SapODataConstants.MultiPolygon.COORDINATES_FIELD_NAME, coordinates)
      .build();
  }

  private String extractDateTimeOffset(String fieldName, Object value) {
    try {
      return EdmDateTimeOffset.getInstance().valueToString(value, EdmLiteralKind.DEFAULT, null);
    } catch (EdmSimpleTypeException e) {
      throw new UnexpectedFormatException(String.format("Unsupported value for '%s' field: '%s'", fieldName, value), e);
    }
  }

  private String extractDuration(String fieldName, BigDecimal decimal) {
    try {
      int precision = decimal.precision();
      int scale = decimal.scale();
      return EdmDuration.getInstance().valueToString(decimal, true, null, precision, scale, true);
    } catch (EdmPrimitiveTypeException e) {
      String errorMessage = String.format("Unsupported value for '%s' field: '%s'", fieldName, decimal);
      throw new UnexpectedFormatException(errorMessage, e);
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
   * <p>
   * For more information see:
   * <a href="https://olingo.apache.org/javadoc/odata4/org/apache/olingo/commons/api/edm/EdmPrimitiveType.html">
   * EdmPrimitiveType
   * </a>
   *
   * @param value  'EDM.Decimal' value of one of the following Java types {@link BigDecimal}, {@link BigInteger},
   *               {@link Double}, {@link Float}, {@link Byte}, {@link Short}, {@link Integer}, {@link Long}.
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
