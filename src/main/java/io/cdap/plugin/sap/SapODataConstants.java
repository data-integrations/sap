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

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

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
   * Configuration property name used to specify whether the plugin should read metadata annotations and include them
   * to each CDAP record.
   */
  public static final String INCLUDE_METADATA_ANNOTATIONS = "includeMetadataAnnotations";

  /**
   * When metadata annotations are included(using {@link SapODataConstants#INCLUDE_METADATA_ANNOTATIONS}), each
   * property is mapped to a CDAP 'record' with exactly two fields: "{@value SapODataConstants#VALUE_FIELD_NAME}" for
   * value and "{@value SapODataConstants#METADATA_ANNOTATIONS_FIELD_NAME}" for metadata annotations.
   */
  public static final String VALUE_FIELD_NAME = "value";

  /**
   * When metadata annotations are included(using {@link SapODataConstants#INCLUDE_METADATA_ANNOTATIONS}), each
   * property is mapped to a CDAP 'record' with exactly two fields: "{@value SapODataConstants#VALUE_FIELD_NAME}" for
   * value and "{@value SapODataConstants#METADATA_ANNOTATIONS_FIELD_NAME}" for metadata annotations.
   */
  public static final String METADATA_ANNOTATIONS_FIELD_NAME = "metadata-annotations";

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

  /**
   * OData 4 metadata annotations mapped to CDAP record with field
   * "{@value SapODataConstants.Annotation#TERM_FIELD_NAME}" for a term name,
   * "{@value SapODataConstants.Annotation#QUALIFIER_FIELD_NAME}" for a qualifier name,
   * "{@value SapODataConstants.Annotation#EXPRESSION_FIELD_NAME}" for an expression record.
   * "{@value SapODataConstants.Annotation#ANNOTATIONS_FIELD_NAME}" for a nested annotation record.
   */
  public static class Annotation {
    public static final String TERM_FIELD_NAME = "term";
    public static final String QUALIFIER_FIELD_NAME = "qualifier";
    public static final String EXPRESSION_FIELD_NAME = "expression";
    public static final String ANNOTATIONS_FIELD_NAME = "annotations";

    public static Schema schema(String name, @Nullable Schema expressionSchema, @Nullable Schema annotationsSchema) {
      List<Schema.Field> fields = new ArrayList<>();
      fields.add(Schema.Field.of(TERM_FIELD_NAME, Schema.of(Schema.Type.STRING)));
      fields.add(Schema.Field.of(QUALIFIER_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
      if (expressionSchema != null) {
        fields.add(Schema.Field.of(EXPRESSION_FIELD_NAME, expressionSchema));
      }
      if (annotationsSchema != null) {
        fields.add(Schema.Field.of(ANNOTATIONS_FIELD_NAME, annotationsSchema));
      }

      return Schema.recordOf(name + "-annotation", fields);
    }
  }

  /**
   * OData 4 metadata annotation expression mapped to CDAP record with field
   * "{@value SapODataConstants.Expression#NAME_FIELD_NAME}" for an expression name.
   */
  public static class Expression {
    public static final String NAME_FIELD_NAME = "name";
  }

  /**
   * Some of the OData 4 metadata annotation expression mapped to CDAP record with field
   * "{@value ValuedExpression#VALUE_FIELD_NAME}" for an expression value.
   * These expressions include:
   * - Constant expressions
   * - Path
   * - AnnotationPath
   * - LabeledElementReference
   * - Null
   * - NavigationPropertyPath
   * - PropertyPath
   * - Not
   * - Cast
   * - IsOf
   * - LabeledElement
   * - UrlRef
   */
  public static class ValuedExpression extends Expression {
    public static final String VALUE_FIELD_NAME = "value";

    public static final Schema SCHEMA = Schema.recordOf(
      "single-value-expression",
      Schema.Field.of(NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(VALUE_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
  }

  /**
   * Some of the OData 4 metadata annotation expression mapped to CDAP record with field
   * "{@value SapODataConstants.TypedExpression#TYPE_FIELD_NAME}" for a type name.
   * These expressions include:
   * - Cast
   * - IsOf
   * - Record
   */
  public static class TypedExpression extends ValuedExpression {
    public static final String TYPE_FIELD_NAME = "type";
  }

  /**
   * OData 4 expressions may be annotated.
   */
  public static class AnnotatedExpression extends TypedExpression {
    public static final String ANNOTATIONS_FIELD_NAME = "annotations";
  }

  /**
   * OData 4 "Apply" metadata annotation expression mapped to CDAP record with field
   * "{@value SapODataConstants.ApplyExpression#FUNCTION_FIELD_NAME}" for a function name,
   * "{@value SapODataConstants.ApplyExpression#PARAMETERS_FIELD_NAME}" for a parameters record.
   */
  public static class ApplyExpression extends Expression {
    public static final String FUNCTION_FIELD_NAME = "function";
    public static final String PARAMETERS_FIELD_NAME = "parameters";

    public static Schema schema(String name, Schema parametersSchema) {
      return Schema.recordOf(name + "-apply-expression",
                             Schema.Field.of(NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
                             Schema.Field.of(FUNCTION_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                             Schema.Field.of(PARAMETERS_FIELD_NAME, parametersSchema));
    }
  }

  /**
   * OData 4 logical & comparison expressions mapped to CDAP record with field
   * "{@value SapODataConstants.LogicalExpression#LEFT_FIELD_NAME}" for a left expression record,
   * "{@value SapODataConstants.LogicalExpression#RIGHT_FIELD_NAME}" for a right expression record.
   */
  public static class LogicalExpression extends Expression {
    public static final String LEFT_FIELD_NAME = "left";
    public static final String RIGHT_FIELD_NAME = "right";

    public static Schema schema(String name, Schema left, Schema right) {
      return Schema.recordOf(name + "-logical-expression",
                             Schema.Field.of(NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
                             Schema.Field.of(LEFT_FIELD_NAME, left),
                             Schema.Field.of(RIGHT_FIELD_NAME, right));
    }
  }

  /**
   * OData 4 "Not" expressions mapped to CDAP record with field
   * "{@value SapODataConstants.LogicalExpression#VALUE_FIELD_NAME}" for a value expression record.
   */
  public static class NotExpression extends ValuedExpression {
    public static Schema schema(String name, Schema value) {
      return Schema.recordOf(name + "-not-expression",
                             Schema.Field.of(NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
                             Schema.Field.of(VALUE_FIELD_NAME, value));
    }
  }

  /**
   * OData 4 "Cast" and "IsOf" expressions mapped to CDAP record with field
   * "{@value CastIsOfExpression#MAX_LENGTH_FIELD_NAME}" for a maximum length of value,
   * "{@value CastIsOfExpression#PRECISION_FIELD_NAME}" for a precision of value,
   * "{@value CastIsOfExpression#SCALE_FIELD_NAME}" for a scale of value,
   * "{@value CastIsOfExpression#SRID_FIELD_NAME}" for a SRID of value.
   */
  public static class CastIsOfExpression extends TypedExpression {
    public static final String MAX_LENGTH_FIELD_NAME = "maxLength";
    public static final String PRECISION_FIELD_NAME = "precision";
    public static final String SCALE_FIELD_NAME = "scale";
    public static final String SRID_FIELD_NAME = "srid";

    public static Schema schema(String name, Schema valueSchema) {
      return Schema.recordOf(name + "-cast-is-of-expression",
                             Schema.Field.of(NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
                             Schema.Field.of(TYPE_FIELD_NAME, Schema.of(Schema.Type.STRING)),
                             Schema.Field.of(MAX_LENGTH_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                             Schema.Field.of(PRECISION_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                             Schema.Field.of(SCALE_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                             Schema.Field.of(SRID_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                             Schema.Field.of(VALUE_FIELD_NAME, valueSchema));
    }
  }

  /**
   * OData 4 "Collection" expression mapped to CDAP record with field
   * "{@value SapODataConstants.CollectionExpression#ITEMS_FIELD_NAME}" for an array of item expressions.
   */
  public static class CollectionExpression extends Expression {
    public static final String ITEMS_FIELD_NAME = "items";

    public static Schema schema(String name, @Nullable Schema componentSchema) {
      List<Schema.Field> fields = new ArrayList<>();
      fields.add(Schema.Field.of(NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)));
      if (componentSchema != null) {
        fields.add(Schema.Field.of(ITEMS_FIELD_NAME, Schema.arrayOf(componentSchema)));
      }
      return Schema.recordOf(name + "-collection-expression", fields);
    }
  }

  /**
   * OData 4 "If" expression mapped to CDAP record with field
   * "{@value SapODataConstants.IfExpression#GUARD_FIELD_NAME}" for a guard expression,
   * "{@value SapODataConstants.IfExpression#THEN_FIELD_NAME}" for a then expression,
   * "{@value SapODataConstants.IfExpression#ELSE_FIELD_NAME}" for an else expression.
   */
  public static class IfExpression extends Expression {
    public static final String GUARD_FIELD_NAME = "guard";
    public static final String THEN_FIELD_NAME = "then";
    public static final String ELSE_FIELD_NAME = "else";

    public static Schema schema(String name, Schema guardSchema, Schema thenSchema, Schema elseSchema) {
      return Schema.recordOf(name + "-if-expression",
                             Schema.Field.of(NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
                             Schema.Field.of(GUARD_FIELD_NAME, guardSchema),
                             Schema.Field.of(THEN_FIELD_NAME, thenSchema),
                             Schema.Field.of(ELSE_FIELD_NAME, elseSchema));
    }
  }

  /**
   * OData 4 "LabeledElement" expression mapped to CDAP record with field
   * "{@value SapODataConstants.LabeledElementExpression#ELEMENT_NAME_FIELD_NAME}" for an element name.
   */
  public static class LabeledElementExpression extends ValuedExpression {
    public static final String ELEMENT_NAME_FIELD_NAME = "elementName";

    public static Schema schema(String name, Schema valueSchema) {
      return Schema.recordOf(name + "-labeled-element-expression",
                             Schema.Field.of(NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
                             Schema.Field.of(ELEMENT_NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
                             Schema.Field.of(VALUE_FIELD_NAME, valueSchema));
    }
  }

  /**
   * OData 4 "Record" expression mapped to CDAP record with field
   * "{@value SapODataConstants.RecordExpression#PROPERTY_VALUES_FIELD_NAME}" for a record of property values.
   */
  public static class RecordExpression extends AnnotatedExpression {
    public static final String PROPERTY_VALUES_FIELD_NAME = "propertyValues";

    public static Schema schema(String name, @Nullable Schema propertiesSchema, @Nullable Schema annotationsSchema) {
      List<Schema.Field> fields = new ArrayList<>();
      fields.add(Schema.Field.of(NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)));
      fields.add(Schema.Field.of(TYPE_FIELD_NAME, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
      if (propertiesSchema != null) {
        fields.add(Schema.Field.of(PROPERTY_VALUES_FIELD_NAME, propertiesSchema));
      }
      if (annotationsSchema != null) {
        fields.add(Schema.Field.of(ANNOTATIONS_FIELD_NAME, annotationsSchema));
      }
      return Schema.recordOf(name + "-record-expression", fields);
    }
  }

  /**
   * UrlRef expression.
   */
  public static class UrlRefExpression extends ValuedExpression {
    public static Schema schema(String name, Schema valueSchema) {
      return Schema.recordOf(name + "-url-ref-expression",
                             Schema.Field.of(NAME_FIELD_NAME, Schema.of(Schema.Type.STRING)),
                             Schema.Field.of(VALUE_FIELD_NAME, valueSchema));
    }
  }
}
