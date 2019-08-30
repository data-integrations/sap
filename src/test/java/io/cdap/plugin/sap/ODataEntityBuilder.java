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

package io.cdap.plugin.sap;

import io.cdap.plugin.sap.odata.ODataEntity;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
import org.apache.olingo.commons.api.edm.geo.Geospatial;
import org.apache.olingo.commons.api.edm.geo.GeospatialCollection;
import org.apache.olingo.commons.api.edm.geo.LineString;
import org.apache.olingo.commons.api.edm.geo.MultiLineString;
import org.apache.olingo.commons.api.edm.geo.MultiPoint;
import org.apache.olingo.commons.api.edm.geo.MultiPolygon;
import org.apache.olingo.commons.api.edm.geo.Point;
import org.apache.olingo.commons.api.edm.geo.Polygon;
import org.apache.olingo.commons.api.edm.geo.SRID;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDate;
import org.apache.olingo.commons.core.edm.primitivetype.EdmDuration;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeographyCollection;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeographyLineString;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeographyMultiLineString;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeographyMultiPoint;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeographyMultiPolygon;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeographyPoint;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeographyPolygon;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeometryCollection;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeometryLineString;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeometryMultiLineString;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeometryMultiPoint;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeometryMultiPolygon;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeometryPoint;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeometryPolygon;
import org.apache.olingo.commons.core.edm.primitivetype.EdmTimeOfDay;
import org.apache.olingo.commons.core.edm.primitivetype.SingletonPrimitiveType;
import org.apache.olingo.odata2.api.edm.EdmLiteralKind;
import org.apache.olingo.odata2.api.edm.EdmSimpleTypeException;
import org.apache.olingo.odata2.core.edm.AbstractSimpleType;
import org.apache.olingo.odata2.core.edm.EdmBinary;
import org.apache.olingo.odata2.core.edm.EdmBoolean;
import org.apache.olingo.odata2.core.edm.EdmByte;
import org.apache.olingo.odata2.core.edm.EdmDateTime;
import org.apache.olingo.odata2.core.edm.EdmDateTimeOffset;
import org.apache.olingo.odata2.core.edm.EdmDecimal;
import org.apache.olingo.odata2.core.edm.EdmDouble;
import org.apache.olingo.odata2.core.edm.EdmGuid;
import org.apache.olingo.odata2.core.edm.EdmInt16;
import org.apache.olingo.odata2.core.edm.EdmInt32;
import org.apache.olingo.odata2.core.edm.EdmInt64;
import org.apache.olingo.odata2.core.edm.EdmSByte;
import org.apache.olingo.odata2.core.edm.EdmSingle;
import org.apache.olingo.odata2.core.edm.EdmString;
import org.apache.olingo.odata2.core.edm.EdmTime;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Provides handy methods to construct a {@link ODataEntity} instance for testing.
 */
public final class ODataEntityBuilder {

  private static final SRID TEST_SRID = SRID.valueOf("4326");
  private final Map<String, Object> properties;

  private ODataEntityBuilder() {
    this.properties = new HashMap<>();
  }

  public static ODataEntityBuilder builder() {
    return new ODataEntityBuilder();
  }

  protected ODataEntityBuilder set(String name, Object value, AbstractSimpleType type) {
    try {
      // Trigger validation
      type.valueToString(value, EdmLiteralKind.DEFAULT, null);
    } catch (EdmSimpleTypeException e) {
      throw new IllegalArgumentException(e);
    }
    this.properties.put(name, value);
    return this;
  }

  protected ODataEntityBuilder set(String name, Object value, SingletonPrimitiveType type) {
    return set(name, value, type, null, null);
  }

  protected ODataEntityBuilder set(String name, Object value, SingletonPrimitiveType type, Integer scale,
                                   Integer precision) {
    try {
      // Trigger validation
      type.valueToString(value, true, null, precision, scale, true);
    } catch (EdmPrimitiveTypeException e) {
      throw new IllegalArgumentException(e);
    }
    this.properties.put(name, value);
    return this;
  }

  public ODataEntityBuilder setInt16(String name, short value) {
    return set(name, value, EdmInt16.getInstance());
  }

  public ODataEntityBuilder setInt32(String name, int value) {
    return set(name, value, EdmInt32.getInstance());
  }

  public ODataEntityBuilder setInt64(String name, long value) {
    return set(name, value, EdmInt64.getInstance());
  }

  public ODataEntityBuilder setBinary(String name, byte[] value) {
    return set(name, value, EdmBinary.getInstance());
  }

  public ODataEntityBuilder setBoolean(String name, boolean value) {
    return set(name, value, EdmBoolean.getInstance());
  }

  public ODataEntityBuilder setByte(String name, short value) {
    return set(name, value, EdmByte.getInstance());
  }

  public ODataEntityBuilder setDateTime(String name, Calendar value) {
    return set(name, value, EdmDateTime.getInstance());
  }

  public ODataEntityBuilder setDecimal(String name, BigDecimal value) {
    return set(name, value, EdmDecimal.getInstance());
  }

  public ODataEntityBuilder setDouble(String name, double value) {
    return set(name, value, EdmDouble.getInstance());
  }

  public ODataEntityBuilder setSingle(String name, float value) {
    return set(name, value, EdmSingle.getInstance());
  }

  public ODataEntityBuilder setGuid(String name, UUID value) {
    return set(name, value, EdmGuid.getInstance());
  }

  public ODataEntityBuilder setSByte(String name, byte value) {
    return set(name, value, EdmSByte.getInstance());
  }

  public ODataEntityBuilder setString(String name, String value) {
    return set(name, value, EdmString.getInstance());
  }

  public ODataEntityBuilder setTime(String name, Calendar value) {
    return set(name, value, EdmTime.getInstance());
  }

  public ODataEntityBuilder setDateTimeOffset(String name, String value) throws EdmSimpleTypeException {
    EdmDateTimeOffset edmDateTimeOffset = EdmDateTimeOffset.getInstance();
    Calendar calendar = edmDateTimeOffset.valueOfString(value, EdmLiteralKind.DEFAULT, null, Calendar.class);
    return set(name, calendar, edmDateTimeOffset);
  }

  public ODataEntityBuilder setDate(String name, Timestamp timestamp) throws EdmPrimitiveTypeException {
    return set(name, timestamp, EdmDate.getInstance());
  }

  public ODataEntityBuilder setDate(String name, String value) throws EdmPrimitiveTypeException {
    EdmDate edmDate = EdmDate.getInstance();
    Timestamp timestamp = edmDate.valueOfString(value, true, null, null, null, true, Timestamp.class);
    return set(name, timestamp, edmDate);
  }

  public ODataEntityBuilder setDuration(String name, String value, int scale, int precision)
    throws EdmPrimitiveTypeException {
    EdmDuration edmDuration = EdmDuration.getInstance();
    BigDecimal duration = edmDuration.valueOfString(value, true, null, precision, scale, true, BigDecimal.class);
    return set(name, duration, edmDuration, scale, precision);
  }

  public ODataEntityBuilder setTimeOfDay(String name, Timestamp timestamp) throws EdmPrimitiveTypeException {
    return set(name, timestamp, EdmTimeOfDay.getInstance());
  }

  public ODataEntityBuilder setGeometryPoint(String name, double x, double y) throws EdmPrimitiveTypeException {
    return set(name, pointOf(Geospatial.Dimension.GEOMETRY, x, y), EdmGeometryPoint.getInstance());
  }

  public ODataEntityBuilder setGeographyPoint(String name, double x, double y) throws EdmPrimitiveTypeException {
    return set(name, pointOf(Geospatial.Dimension.GEOGRAPHY, x, y), EdmGeographyPoint.getInstance());
  }

  public ODataEntityBuilder setGeometryLineString(String name, List<List<Double>> coordinates) throws
    EdmPrimitiveTypeException {
    List<Point> points = coordinates.stream()
      .map(c -> pointOf(Geospatial.Dimension.GEOMETRY, c.get(0), c.get(1)))
      .collect(Collectors.toList());
    LineString lineString = new LineString(Geospatial.Dimension.GEOMETRY, TEST_SRID, points);

    return set(name, lineString, EdmGeometryLineString.getInstance());
  }

  public ODataEntityBuilder setGeographyLineString(String name, List<List<Double>> coordinates)
    throws EdmPrimitiveTypeException {
    List<Point> points = coordinates.stream()
      .map(c -> pointOf(Geospatial.Dimension.GEOGRAPHY, c.get(0), c.get(1)))
      .collect(Collectors.toList());
    LineString lineString = new LineString(Geospatial.Dimension.GEOGRAPHY, TEST_SRID, points);

    return set(name, lineString, EdmGeographyLineString.getInstance());
  }

  public ODataEntityBuilder setGeometryMultiPoint(String name, List<List<Double>> coordinates) throws
    EdmPrimitiveTypeException {
    List<Point> points = coordinates.stream()
      .map(c -> pointOf(Geospatial.Dimension.GEOMETRY, c.get(0), c.get(1)))
      .collect(Collectors.toList());
    MultiPoint multiPoint = new MultiPoint(Geospatial.Dimension.GEOMETRY, TEST_SRID, points);

    return set(name, multiPoint, EdmGeometryMultiPoint.getInstance());
  }

  public ODataEntityBuilder setGeographyMultiPoint(String name, List<List<Double>> coordinates)
    throws EdmPrimitiveTypeException {
    List<Point> points = coordinates.stream()
      .map(c -> pointOf(Geospatial.Dimension.GEOGRAPHY, c.get(0), c.get(1)))
      .collect(Collectors.toList());
    MultiPoint multiPoint = new MultiPoint(Geospatial.Dimension.GEOGRAPHY, TEST_SRID, points);

    return set(name, multiPoint, EdmGeographyMultiPoint.getInstance());
  }

  public ODataEntityBuilder setGeometryPolygon(String name, List<List<Double>> exteriorCoordinates,
                                               List<List<Double>> interiorCoordinates)
    throws EdmPrimitiveTypeException {
    List<Point> exterior = exteriorCoordinates.stream()
      .map(c -> pointOf(Geospatial.Dimension.GEOMETRY, c.get(0), c.get(1)))
      .collect(Collectors.toList());

    List<Point> interior = interiorCoordinates.stream()
      .map(c -> pointOf(Geospatial.Dimension.GEOMETRY, c.get(0), c.get(1)))
      .collect(Collectors.toList());

    Polygon polygon = new Polygon(Geospatial.Dimension.GEOMETRY, TEST_SRID, interior, exterior);

    return set(name, polygon, EdmGeometryPolygon.getInstance());
  }

  public ODataEntityBuilder setGeographyPolygon(String name, List<List<Double>> exteriorCoordinates,
                                                List<List<Double>> interiorCoordinates)
    throws EdmPrimitiveTypeException {
    List<Point> exterior = exteriorCoordinates.stream()
      .map(c -> pointOf(Geospatial.Dimension.GEOGRAPHY, c.get(0), c.get(1)))
      .collect(Collectors.toList());

    List<Point> interior = interiorCoordinates.stream()
      .map(c -> pointOf(Geospatial.Dimension.GEOGRAPHY, c.get(0), c.get(1)))
      .collect(Collectors.toList());

    Polygon polygon = new Polygon(Geospatial.Dimension.GEOGRAPHY, TEST_SRID, interior, exterior);

    return set(name, polygon, EdmGeographyPolygon.getInstance());
  }

  public ODataEntityBuilder setGeometryMultiLineString(String name, List<List<List<Double>>> coordinates) throws
    EdmPrimitiveTypeException {

    List<LineString> lineStringList = new ArrayList<>();
    for (List<List<Double>> lineStringCoordinates : coordinates) {
      List<Point> points = lineStringCoordinates.stream()
        .map(c -> pointOf(Geospatial.Dimension.GEOMETRY, c.get(0), c.get(1)))
        .collect(Collectors.toList());
      LineString lineString = new LineString(Geospatial.Dimension.GEOMETRY, TEST_SRID, points);
      lineStringList.add(lineString);
    }
    MultiLineString multiLS = new MultiLineString(Geospatial.Dimension.GEOMETRY, TEST_SRID, lineStringList);

    return set(name, multiLS, EdmGeometryMultiLineString.getInstance());
  }

  public ODataEntityBuilder setGeographyMultiLineString(String name, List<List<List<Double>>> coordinates) throws
    EdmPrimitiveTypeException {

    List<LineString> lineStringList = new ArrayList<>();
    for (List<List<Double>> lineStringCoordinates : coordinates) {
      List<Point> points = lineStringCoordinates.stream()
        .map(c -> pointOf(Geospatial.Dimension.GEOGRAPHY, c.get(0), c.get(1)))
        .collect(Collectors.toList());
      LineString lineString = new LineString(Geospatial.Dimension.GEOGRAPHY, TEST_SRID, points);
      lineStringList.add(lineString);
    }
    MultiLineString multiLS = new MultiLineString(Geospatial.Dimension.GEOGRAPHY, TEST_SRID, lineStringList);

    return set(name, multiLS, EdmGeographyMultiLineString.getInstance());
  }

  public ODataEntityBuilder setGeometryMultiPolygon(String name, List<List<List<List<Double>>>> coordinates)
    throws EdmPrimitiveTypeException {

    List<Polygon> polygonList = new ArrayList<>();

    for (List<List<List<Double>>> polygonCoordinates : coordinates) {
      List<Point> exteriror = polygonCoordinates.get(0).stream()
        .map(c -> pointOf(Geospatial.Dimension.GEOMETRY, c.get(0), c.get(1)))
        .collect(Collectors.toList());

      List<Point> interiror = polygonCoordinates.get(1).stream()
        .map(c -> pointOf(Geospatial.Dimension.GEOMETRY, c.get(0), c.get(1)))
        .collect(Collectors.toList());

      Polygon polygon = new Polygon(Geospatial.Dimension.GEOMETRY, TEST_SRID, interiror, exteriror);
      polygonList.add(polygon);
    }
    MultiPolygon multiPolygon = new MultiPolygon(Geospatial.Dimension.GEOMETRY, TEST_SRID, polygonList);

    return set(name, multiPolygon, EdmGeometryMultiPolygon.getInstance());
  }

  public ODataEntityBuilder setGeographyMultiPolygon(String name, List<List<List<List<Double>>>> coordinates)
    throws EdmPrimitiveTypeException {

    List<Polygon> polygonList = new ArrayList<>();

    for (List<List<List<Double>>> polygonCoordinates : coordinates) {
      List<Point> exteriror = polygonCoordinates.get(0).stream()
        .map(c -> pointOf(Geospatial.Dimension.GEOGRAPHY, c.get(0), c.get(1)))
        .collect(Collectors.toList());

      List<Point> interiror = polygonCoordinates.get(1).stream()
        .map(c -> pointOf(Geospatial.Dimension.GEOGRAPHY, c.get(0), c.get(1)))
        .collect(Collectors.toList());

      Polygon polygon = new Polygon(Geospatial.Dimension.GEOGRAPHY, TEST_SRID, interiror, exteriror);
      polygonList.add(polygon);
    }
    MultiPolygon multiPolygon = new MultiPolygon(Geospatial.Dimension.GEOGRAPHY, TEST_SRID, polygonList);

    return set(name, multiPolygon, EdmGeographyMultiPolygon.getInstance());
  }

  public ODataEntityBuilder setGeometryCollection(String name, GeospatialCollection collection)
    throws EdmPrimitiveTypeException {
    return set(name, collection, EdmGeometryCollection.getInstance());
  }

  public ODataEntityBuilder setGeographyCollection(String name, GeospatialCollection collection)
    throws EdmPrimitiveTypeException {
    return set(name, collection, EdmGeographyCollection.getInstance());
  }

  private Point pointOf(Geospatial.Dimension dimension, double x, double y) {
    Point point = new Point(dimension, TEST_SRID);
    point.setX(x);
    point.setY(y);

    return point;
  }

  public ODataEntity build() {
    return new ODataEntity(properties);
  }
}
