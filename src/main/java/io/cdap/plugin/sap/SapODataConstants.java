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

import io.cdap.cdap.api.data.schema.Schema;

/**
 * SAP OData constants.
 */
public class SapODataConstants {

  private SapODataConstants() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * SAP plugin name.
   */
  public static final String PLUGIN_NAME = "SapOData";

  /**
   * Configuration property name used to specify URL of the SAP OData service.
   */
  public static final String ODATA_SERVICE_URL = "url";

  /**
   * Configuration property name used to specify path of the SAP OData entity.
   */
  public static final String RESOURCE_PATH = "resourcePath";

  /**
   * Configuration property name used to specify OData query options to filter the data.
   */
  public static final String QUERY = "query";

  /**
   * Configuration property name used to specify username for basic authentication.
   */
  public static final String USERNAME = "username";

  /**
   * Configuration property name used to specify password for basic authentication.
   */
  public static final String PASSWORD = "password";

  /**
   * Configuration property name used to specify the schema of the entries.
   */
  public static final String SCHEMA = "schema";

  /**
   * OData 4 geospatial data types are mapped to CDAP record with fields
   * "{@value SapODataConstants.Geospatial#DIMENSION_FIELD_NAME}" for dimension.
   */
  public static class Geospatial {
    public static final String DIMENSION_FIELD_NAME = "dimension";
  }

  /**
   * OData 4 geospatial data types are mapped to CDAP record with fields
   * "{@value SapODataConstants.TypedGeospatial#TYPE_FIELD_NAME}" for type
   * and "{@value SapODataConstants.TypedGeospatial#COORDINATES_FIELD_NAME}" for list of coordinates.
   * For instance, "LineString" and "MultiPoint" schemas are the same. Type name is required to distinguish them.
   */
  public static class TypedGeospatial extends Geospatial {
    public static final String TYPE_FIELD_NAME = "type";
    public static final String COORDINATES_FIELD_NAME = "coordinates";
  }

  /**
   * OData 4 geospatial "Point" is mapped to CDAP record with
   * "{@value SapODataConstants.Geospatial#DIMENSION_FIELD_NAME}" for dimension,
   * "{@value SapODataConstants.Point#X_FIELD_NAME}",
   * "{@value SapODataConstants.Point#Y_FIELD_NAME}" and
   * "{@value SapODataConstants.Point#Z_FIELD_NAME}" fields for coordinates.
   */
  public static class Point extends Geospatial {
    public static final String X_FIELD_NAME = "x";
    public static final String Y_FIELD_NAME = "y";
    public static final String Z_FIELD_NAME = "z";

    public static final Schema SCHEMA = Schema.recordOf(
      "point-record",
      Schema.Field.of(DIMENSION_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(X_FIELD_NAME, Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of(Y_FIELD_NAME, Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of(Z_FIELD_NAME, Schema.of(Schema.Type.DOUBLE)));
  }

  /**
   * OData 4 geospatial "LineString" is mapped to CDAP record with
   * "{@value SapODataConstants.TypedGeospatial#TYPE_FIELD_NAME}" for type,
   * "{@value SapODataConstants.Geospatial#DIMENSION_FIELD_NAME}" for dimension and
   * "{@value SapODataConstants.TypedGeospatial#COORDINATES_FIELD_NAME}" for list of coordinates.
   */
  public static class LineString extends TypedGeospatial {
    public static final Schema SCHEMA = Schema.recordOf(
      "line-string-record",
      Schema.Field.of(TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(DIMENSION_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(COORDINATES_FIELD_NAME, Schema.arrayOf(Point.SCHEMA)));
  }

  /**
   * OData 4 geospatial "Polygon" is mapped to CDAP record with
   * "{@value SapODataConstants.Geospatial#DIMENSION_FIELD_NAME}" for dimension and
   * "{@value SapODataConstants.Polygon#EXTERIOR_FIELD_NAME}" field for the exterior coordinates,
   * "{@value SapODataConstants.Polygon#INTERIOR_FIELD_NAME}" field for the interior coordinates,
   * "{@value SapODataConstants.Polygon#NUMBER_OF_INTERIOR_RINGS_FIELD_NAME}" field for the number of interior rings.
   */
  public static class Polygon extends TypedGeospatial {
    public static final String EXTERIOR_FIELD_NAME = "exterior";
    public static final String INTERIOR_FIELD_NAME = "interior";
    public static final String NUMBER_OF_INTERIOR_RINGS_FIELD_NAME = "numberOfInteriorRings";

    public static final Schema SCHEMA = Schema.recordOf(
      "polygon-record",
      Schema.Field.of(TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(DIMENSION_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(EXTERIOR_FIELD_NAME, Schema.arrayOf(Point.SCHEMA)),
      Schema.Field.of(INTERIOR_FIELD_NAME, Schema.arrayOf(LineString.SCHEMA)),
      Schema.Field.of(NUMBER_OF_INTERIOR_RINGS_FIELD_NAME, Schema.of(Schema.Type.INT)));
  }

  /**
   * OData 4 geospatial "MultiPoint" is mapped to CDAP record with
   * "{@value SapODataConstants.TypedGeospatial#TYPE_FIELD_NAME}" for type,
   * "{@value SapODataConstants.Geospatial#DIMENSION_FIELD_NAME}" for dimension and
   * "{@value SapODataConstants.TypedGeospatial#COORDINATES_FIELD_NAME}" for list of coordinates.
   */
  public static class MultiPoint extends TypedGeospatial {
    public static final Schema SCHEMA = Schema.recordOf(
      "multi-point-record",
      Schema.Field.of(TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(DIMENSION_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(COORDINATES_FIELD_NAME, Schema.arrayOf(Point.SCHEMA)));
  }

  /**
   * OData 4 geospatial "MultiLineString" is mapped to CDAP record with
   * "{@value SapODataConstants.TypedGeospatial#TYPE_FIELD_NAME}" for type,
   * "{@value SapODataConstants.Geospatial#DIMENSION_FIELD_NAME}" for dimension and
   * "{@value SapODataConstants.TypedGeospatial#COORDINATES_FIELD_NAME}" for list of coordinates.
   */
  public static class MultiLineString extends TypedGeospatial {
    public static final Schema SCHEMA = Schema.recordOf(
      "multi-line-string-record",
      Schema.Field.of(TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(DIMENSION_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(COORDINATES_FIELD_NAME, Schema.arrayOf(LineString.SCHEMA)));
  }

  /**
   * OData 4 geospatial "MultiPolygon" is mapped to CDAP record with
   * "{@value SapODataConstants.TypedGeospatial#TYPE_FIELD_NAME}" for type,
   * "{@value SapODataConstants.Geospatial#DIMENSION_FIELD_NAME}" for dimension and
   * "{@value SapODataConstants.TypedGeospatial#COORDINATES_FIELD_NAME}" for list of coordinates.
   */
  public static class MultiPolygon extends TypedGeospatial {
    public static final Schema SCHEMA = Schema.recordOf(
      "multi-polygon-record",
      Schema.Field.of(TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(DIMENSION_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(COORDINATES_FIELD_NAME, Schema.arrayOf(Polygon.SCHEMA)));
  }

  /**
   * OData 4 geospatial collection is mapped to CDAP record with
   * "{@value SapODataConstants.Geospatial#DIMENSION_FIELD_NAME}" for a dimension,
   * "{@value GeospatialCollection#POINTS_FIELD_NAME}" for a list of geospatial "Point" values,
   * "{@value GeospatialCollection#LINE_STRINGS_FIELD_NAME}" for a list of geospatial "LineString" values,
   * "{@value GeospatialCollection#POLYGONS_FIELD_NAME}" for a list of geospatial "Polygon" values,
   * "{@value GeospatialCollection#MULTI_POINTS_FIELD_NAME}" for a list of geospatial "MultiPoint" values,
   * "{@value GeospatialCollection#MULTI_LINE_STRINGS_FIELD_NAME}" for a list of geospatial "MultiLineString" values,
   * "{@value GeospatialCollection#MULTI_POLYGONS_FIELD_NAME}" for a list of geospatial "MultiPolygon" values.
   */
  public static class GeospatialCollection extends TypedGeospatial {
    public static final String POINTS_FIELD_NAME = "points";
    public static final String LINE_STRINGS_FIELD_NAME = "lineStrings";
    public static final String POLYGONS_FIELD_NAME = "polygons";
    public static final String MULTI_POINTS_FIELD_NAME = "multiPoints";
    public static final String MULTI_LINE_STRINGS_FIELD_NAME = "multiLineStrings";
    public static final String MULTI_POLYGONS_FIELD_NAME = "multiPolygons";

    public static final Schema SCHEMA = Schema.recordOf(
      "collection-record",
      Schema.Field.of(TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(DIMENSION_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(POINTS_FIELD_NAME, Schema.arrayOf(Point.SCHEMA)),
      Schema.Field.of(LINE_STRINGS_FIELD_NAME, Schema.arrayOf(LineString.SCHEMA)),
      Schema.Field.of(POLYGONS_FIELD_NAME, Schema.arrayOf(Polygon.SCHEMA)),
      Schema.Field.of(MULTI_POINTS_FIELD_NAME, Schema.arrayOf(MultiPoint.SCHEMA)),
      Schema.Field.of(MULTI_LINE_STRINGS_FIELD_NAME, Schema.arrayOf(MultiLineString.SCHEMA)),
      Schema.Field.of(MULTI_POLYGONS_FIELD_NAME, Schema.arrayOf(MultiPolygon.SCHEMA))
      // nested collections can not be supported since metadata does not contain component info
    );
  }

  /**
   * OData 4 Stream is mapped to CDAP record with
   * "{@value SapODataConstants.Stream#ETAG_FIELD_NAME}" for the ETag of the stream,
   * "{@value SapODataConstants.Stream#CONTENT_TYPE_FIELD_NAME}" for the media type of the stream,
   * "{@value SapODataConstants.Stream#READ_LINK_FIELD_NAME}" for the link used to read the stream,
   * "{@value SapODataConstants.Stream#EDIT_LINK_FIELD_NAME}" for the link used to edit/update the stream.
   */
  public static class Stream {
    public static final String ETAG_FIELD_NAME = "mediaEtag";
    public static final String CONTENT_TYPE_FIELD_NAME = "mediaContentType";
    public static final String READ_LINK_FIELD_NAME = "mediaReadLink";
    public static final String EDIT_LINK_FIELD_NAME = "mediaEditLink";

    public static final Schema SCHEMA = Schema.recordOf(
      "stream-record",
      Schema.Field.of(ETAG_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(CONTENT_TYPE_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(READ_LINK_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of(EDIT_LINK_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))));

  }
}
