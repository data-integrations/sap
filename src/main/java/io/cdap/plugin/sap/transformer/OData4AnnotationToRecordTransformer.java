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
import io.cdap.plugin.sap.odata.ODataUtil;
import io.cdap.plugin.sap.odata.odata4.OData4Annotation;
import org.apache.olingo.commons.api.edm.annotation.EdmExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlApply;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlCast;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlCollection;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlIf;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlIsOf;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlLabeledElement;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlLogicalOrComparisonExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlPropertyValue;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlRecord;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlUrlRef;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Transforms {@link OData4Annotation} to {@link StructuredRecord} according to the specified schema.
 */
public class OData4AnnotationToRecordTransformer {

  /**
   * Transforms {@link OData4Annotation} to {@link StructuredRecord} according to the specified schema.
   * An annotation applies a term to a model element and defines how to calculate a value for the term application.
   * The value of an annotation is specified as an annotation expression, which is either a constant expression
   * representing a constant value or a dynamic expression.
   * OData V4 metadata annotations are mapped to a record of the following fields:
   * <ul>
   * <li>{@value SapODataConstants.Annotation#TERM_FIELD_NAME} - a simple identifier, such as "UI.DisplayName" or
   * "Core.Description", etc.
   * <li>{@value SapODataConstants.Annotation#QUALIFIER_FIELD_NAME} - a term can be applied multiple times to the same
   * model element by providing a qualifier to distinguish the annotations.
   * <li>{@value SapODataConstants.Annotation#EXPRESSION_FIELD_NAME} - record that corresponds to a constant expression
   * or a dynamic expression.
   * <li>{@value SapODataConstants.Annotation#ANNOTATIONS_FIELD_NAME} - record that corresponds to nested annotations.
   * </ul>
   * <br/>
   * For example, given annotation with constant expression, which has the following XML representation:
   * <p>
   * {@code
   * <Annotation Term="Measures.ISOCurrency" String="USD">
   * <Annotation Term="Core.Description" String="Nested annotation" />
   * </Annotation>
   * }
   * </p>
   * will be transformed to the following {@link StructuredRecord}:
   * <pre>
   *   term: "Measures.ISOCurrency"
   *   qualifier: null
   *   expression:
   *      name: "String"
   *      value: "USD"
   *   annotations:
   *      core_description:
   *          term: "Core.Description"
   *          qualifier: null
   *          expression:
   *              name: "String"
   *              value: "Nested annotation"
   * </pre>
   *
   * @param annotation OData 4 annotation.
   * @param schema     annotation record schema.
   * @return {@link StructuredRecord} that corresponds to the given {@link OData4Annotation}.
   */
  public StructuredRecord transform(OData4Annotation annotation, Schema schema) {
    return extractAnnotation(annotation, schema);
  }

  private StructuredRecord extractAnnotation(OData4Annotation annotation, Schema schema) {
    if (annotation == null) {
      return null;
    }

    StructuredRecord.Builder builder = StructuredRecord.builder(schema)
      .set(SapODataConstants.Annotation.TERM_FIELD_NAME, annotation.getTerm())
      .set(SapODataConstants.Annotation.QUALIFIER_FIELD_NAME, annotation.getQualifier());
    Schema.Field expressionField = schema.getField(SapODataConstants.Annotation.EXPRESSION_FIELD_NAME);
    if (expressionField != null) {
      Schema expressionSchema = expressionField.getSchema().isNullable() ? expressionField.getSchema().getNonNullable()
        : expressionField.getSchema();
      CsdlExpression expression = annotation.getExpression();
      StructuredRecord expressionRecord = extractExpression(annotation.getName(), expression, expressionSchema);
      builder.set(SapODataConstants.Annotation.EXPRESSION_FIELD_NAME, expressionRecord);
    }

    Map<String, OData4Annotation> nestedAnnotationsByName = annotation.getAnnotations();
    if (nestedAnnotationsByName.isEmpty()) {
      return builder.build();
    }

    Schema.Field nestedAnnotationsField = schema.getField(SapODataConstants.Annotation.ANNOTATIONS_FIELD_NAME);
    Schema annotationsSchema = nestedAnnotationsField.getSchema().isNullable()
      ? nestedAnnotationsField.getSchema().getNonNullable() : nestedAnnotationsField.getSchema();
    StructuredRecord annotationsRecord = extractNestedAnnotations(nestedAnnotationsByName, annotationsSchema);
    return builder
      .set(SapODataConstants.Annotation.ANNOTATIONS_FIELD_NAME, annotationsRecord)
      .build();
  }

  /**
   * Extracts nested annotations record. Nested annotations mapped to a record where each field corresponds to an
   * annotation. Field names are obtained using {@link OData4Annotation#getName()}.
   * <br/>
   * For example, for annotation which has two nested annotations:
   * <p>
   * {@code
   * <Annotation Term="Measures.ISOCurrency" String="USD">
   * <Annotation Term="Core.Description" String="Nested annotation" />
   * <Annotation Term="Core.Description" Qualifier="Second" String="Second nested annotation" />
   * </Annotation>
   * }
   * </p>
   * nested annotations will be mapped to the following {@link StructuredRecord} with two fields:
   * <pre>
   *   core_description:
   *    term: "Core.Description"
   *    qualifier: null
   *    expression:
   *      name: "String"
   *      value: "Nested annotation"
   *   second_core_description:
   *    term: "Core.Description"
   *    qualifier: "Second"
   *    expression:
   *      name: "String"
   *      value: "Second nested annotation"
   * </pre>
   *
   * @param annotations map of nested annotations by name.
   * @param schema      nested annotations record schema.
   * @return nested annotations record that corresponds to the given map of nested annotations by name.
   */
  private StructuredRecord extractNestedAnnotations(Map<String, OData4Annotation> annotations, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    schema.getFields().forEach(f -> {
      Schema annotationSchema = f.getSchema().isNullable() ? f.getSchema().getNonNullable() : f.getSchema();
      OData4Annotation nestedAnnotation = annotations.get(f.getName());
      builder.set(f.getName(), extractAnnotation(nestedAnnotation, annotationSchema));
    });

    return builder.build();
  }

  private StructuredRecord extractExpression(String fieldName, CsdlExpression expression, Schema schema) {
    EdmExpression.EdmExpressionType type = ODataUtil.typeOf(expression);
    switch (type) {
      case Null:
      case Binary:
      case Bool:
      case Date:
      case DateTimeOffset:
      case Decimal:
      case Duration:
      case EnumMember:
      case Float:
      case Guid:
      case Int:
      case String:
      case TimeOfDay:
        String constantExpressionValue = expression.asConstant().getValue();
        String typeName = expression.asConstant().getType().name();
        return extractSingleValueExpression(typeName, constantExpressionValue, schema);
      case Path:
        String pathValue = expression.asDynamic().asPath().getValue();
        String pathType = EdmExpression.EdmExpressionType.Path.name();
        return extractSingleValueExpression(pathType, pathValue, schema);
      case AnnotationPath:
        String annotationPathValue = expression.asDynamic().asAnnotationPath().getValue();
        String annotationPathType = EdmExpression.EdmExpressionType.AnnotationPath.name();
        return extractSingleValueExpression(annotationPathType, annotationPathValue, schema);
      case LabeledElementReference:
        String labeledElementReferenceValue = expression.asDynamic().asLabeledElementReference().getValue();
        String labeledElementReferenceType = EdmExpression.EdmExpressionType.LabeledElementReference.name();
        return extractSingleValueExpression(labeledElementReferenceType, labeledElementReferenceValue, schema);
      case NavigationPropertyPath:
        String navigationPropertyPathValue = expression.asDynamic().asNavigationPropertyPath().getValue();
        String navigationPropertyPathType = EdmExpression.EdmExpressionType.NavigationPropertyPath.name();
        return extractSingleValueExpression(navigationPropertyPathType, navigationPropertyPathValue, schema);
      case PropertyPath:
        String propertyPathValue = expression.asDynamic().asPropertyPath().getValue();
        String propertyPathType = EdmExpression.EdmExpressionType.PropertyPath.name();
        return extractSingleValueExpression(propertyPathType, propertyPathValue, schema);
      case And:
      case Or:
      case Eq:
      case Ne:
      case Gt:
      case Ge:
      case Lt:
      case Le:
        // Annotations on logical & comparison expressions are not included.
        // Olingo V4 client does not parse expression annotations correctly:
        // https://issues.apache.org/jira/browse/OLINGO-1403
        CsdlLogicalOrComparisonExpression logicalOrComparison = expression.asDynamic().asLogicalOrComparison();
        return extractLogicalExpression(fieldName, logicalOrComparison, schema);
      case Not:
        // Negation expressions are represented as an element edm:Not that MUST contain a single annotation expression.
        // See 'Comparison and Logical Operators' section of
        // https://docs.oasis-open.org/odata/odata-csdl-xml/v4.01/csprd05/odata-csdl-xml-v4.01-csprd05.html
        // However, Olingo's EdmNot interface extends common interface for logical or comparison expressions.
        // Thus, value expression can be accessed via either 'getLeftExpression' or 'getRightExpression'.
        // See: AbstractEdmLogicalOrComparisonExpression#getRightExpression implementation for details
        CsdlExpression value = expression.asDynamic().asLogicalOrComparison().getLeft();
        return extractNotExpression(fieldName, value, schema);
      case Apply:
        return extractApplyExpression(fieldName, expression.asDynamic().asApply(), schema);
      case Cast:
        return extractCastExpression(fieldName, expression.asDynamic().asCast(), schema);
      case Collection:
        return extractCollectionExpression(fieldName, expression.asDynamic().asCollection(), schema);
      case If:
        return extractIfExpression(fieldName, expression.asDynamic().asIf(), schema);
      case IsOf:
        return extractIsOfExpression(fieldName, expression.asDynamic().asIsOf(), schema);
      case LabeledElement:
        return extractLabeledElementExpression(fieldName, expression.asDynamic().asLabeledElement(), schema);
      case Record:
        return extractRecordExpression(expression.asDynamic().asRecord(), schema);
      case UrlRef:
        return extractUrlRefExpression(fieldName, expression.asDynamic().asUrlRef(), schema);
      default:
        // this should never happen
        throw new UnexpectedFormatException(
          String.format("Annotation expression for field '%s' is of unsupported type '%s'.", fieldName, type));
    }
  }

  /**
   * Extracts logical expression record. OData 4 logical & comparison expressions mapped to a record with field
   * "{@value SapODataConstants.LogicalExpression#NAME_FIELD_NAME}" for expression name,
   * "{@value SapODataConstants.LogicalExpression#LEFT_FIELD_NAME}" for a left expression record,
   * "{@value SapODataConstants.LogicalExpression#RIGHT_FIELD_NAME}" for a right expression record.
   * <br/>
   * For example, for annotation with 'And' expression:
   * <p>
   * {@code
   * <Annotation Term="Core.Description">
   * <And>
   * <Path>BooleanProperty1</Path>
   * <Path>BooleanProperty2</Path>
   * </And>
   * </Annotation>
   * }
   * </p>
   * expression will be mapped to the following {@link StructuredRecord}:
   * <pre>
   *   name: "And"
   *   left:
   *    name: "Path"
   *    value: "BooleanProperty1"
   *   right:
   *    name: "Path"
   *    value: "BooleanProperty2"
   * </pre>
   * See "Comparison and Logical Operators" section of
   * <a href="https://docs.oasis-open.org/odata/odata-csdl-xml/v4.01/csprd05/odata-csdl-xml-v4.01-csprd05.html">
   * OData JSON Format Version 4.01
   * </a> document.
   *
   * @param fieldName annotation field name.
   * @param logical   logical or comparison expression.
   * @param schema    logical expression record schema.
   * @return logical expression record that corresponds to the given logical expression.
   */
  private StructuredRecord extractLogicalExpression(String fieldName, CsdlLogicalOrComparisonExpression logical,
                                                    Schema schema) {
    CsdlExpression left = logical.getLeft();
    CsdlExpression right = logical.getRight();
    Schema leftFieldSchema = schema.getField(SapODataConstants.LogicalExpression.LEFT_FIELD_NAME).getSchema();
    Schema rightFieldSchema = schema.getField(SapODataConstants.LogicalExpression.RIGHT_FIELD_NAME).getSchema();
    StructuredRecord leftRecord = extractExpression(fieldName, left, leftFieldSchema);
    StructuredRecord rightRecord = extractExpression(fieldName, right, rightFieldSchema);
    return StructuredRecord.builder(schema)
      .set(SapODataConstants.LogicalExpression.NAME_FIELD_NAME, logical.getType().name())
      .set(SapODataConstants.LogicalExpression.LEFT_FIELD_NAME, leftRecord)
      .set(SapODataConstants.LogicalExpression.RIGHT_FIELD_NAME, rightRecord)
      .build();
  }

  /**
   * Extracts "Not" expression record. OData 4 "Not" expressions mapped to a record with field
   * "{@value SapODataConstants.LogicalExpression#NAME_FIELD_NAME}" for expression name,
   * "{@value SapODataConstants.LogicalExpression#VALUE_FIELD_NAME}" for a value expression record.
   * <br/>
   * For example, for annotation with 'Not' expression:
   * <p>
   * {@code
   * <Annotation Term="Core.Description">
   * <Not>
   * <Path>SomeProperty</Path>
   * </Not>
   * </Annotation>
   * }
   * </p>
   * expression will be mapped to the following {@link StructuredRecord}:
   * <pre>
   *   name: "Not"
   *   value:
   *    name: "Path"
   *    value: "SomeProperty"
   * </pre>
   * See "Comparison and Logical Operators" section of
   * <a href="https://docs.oasis-open.org/odata/odata-csdl-xml/v4.01/csprd05/odata-csdl-xml-v4.01-csprd05.html">
   * OData JSON Format Version 4.01
   * </a> document.
   *
   * @param fieldName annotation field name.
   * @param value     value expression.
   * @param schema    "Not" expression record schema.
   * @return "Not" expression record that corresponds to the given logical expression.
   */
  private StructuredRecord extractNotExpression(String fieldName, CsdlExpression value, Schema schema) {
    Schema leftFieldSchema = schema.getField(SapODataConstants.NotExpression.VALUE_FIELD_NAME).getSchema();
    return StructuredRecord.builder(schema)
      .set(SapODataConstants.NotExpression.NAME_FIELD_NAME, EdmExpression.EdmExpressionType.Not.name())
      .set(SapODataConstants.NotExpression.VALUE_FIELD_NAME, extractExpression(fieldName, value, leftFieldSchema))
      .build();
  }

  /**
   * Extract single-value expression record. Some of the OData 4 metadata annotation expression mapped to a record with
   * field "{@value SapODataConstants.ValuedExpression#NAME_FIELD_NAME}" for expression name,
   * field "{@value SapODataConstants.ValuedExpression#VALUE_FIELD_NAME}" for an expression value.
   * These expressions include:
   * - Constant expressions
   * - Path
   * - AnnotationPath
   * - LabeledElementReference
   * - Null
   * - NavigationPropertyPath
   * - PropertyPath
   * <br/>
   * For example, for annotation with 'LabeledElementReference' expression:
   * <p>
   * {@code
   * <Annotation Term="org.example.display.DisplayName">
   * <LabeledElementReference>Model.CustomerFirstName</LabeledElementReference>
   * </Annotation>
   * }
   * </p>
   * expression will be mapped to the following {@link StructuredRecord}:
   * <pre>
   *   name: "LabeledElementReference"
   *   value: "Model.CustomerFirstName"
   * </pre>
   *
   * @param expressionName expression name used to distinguish expressions.
   * @param value          expression value.
   * @param schema         expression schema.
   * @return expression record that corresponds to the given single-value expression.
   */
  private StructuredRecord extractSingleValueExpression(String expressionName, Object value, Schema schema) {
    return StructuredRecord.builder(schema)
      .set(SapODataConstants.ValuedExpression.NAME_FIELD_NAME, expressionName)
      .set(SapODataConstants.ValuedExpression.VALUE_FIELD_NAME, value)
      .build();
  }

  /**
   * Extract "Apply" expression record. OData 4 "Apply" metadata annotation expression mapped to CDAP record with field
   * "{@value SapODataConstants.ApplyExpression#NAME_FIELD_NAME}" for expression name,
   * "{@value SapODataConstants.ApplyExpression#FUNCTION_FIELD_NAME}" for a function name,
   * "{@value SapODataConstants.ApplyExpression#PARAMETERS_FIELD_NAME}" for a parameters record. Parameter index is used
   * as a suffix for field name to avoid conflicts.
   * <br/>
   * For example, for annotation with 'Apply' expression:
   * <p>
   * {@code
   * <Annotation Term="Core.Description">
   * <Apply Function="odata.concat">
   * <String>Product:</String>
   * <Path>SomeProperty1</Path>
   * <String>(</String>
   * <Path>SomeProperty2</Path>
   * <String>)</String>
   * </Apply>
   * </Annotation>
   * }
   * </p>
   * expression will be mapped to the following {@link StructuredRecord}:
   * <pre>
   *   name: "Apply"
   *   function: "odata.concat"
   *   parameters:
   *    String_0:
   *      name: "String"
   *      value: "Product:"
   *    Path_1:
   *      name: "Path"
   *      value: "SomeProperty1:"
   *    String_2:
   *      name: "String"
   *      value: "("
   *    Path_3:
   *      name: "String"
   *      value: "SomeProperty2"
   *    String_4
   *      name: "String"
   *      value: ")"
   * </pre>
   *
   * @param fieldName annotation field name.
   * @param apply     "Apply" expression.
   * @param schema    "Apply" expression record schema.
   * @return expression record that corresponds to the given "Apply" expression.
   */
  private StructuredRecord extractApplyExpression(String fieldName, CsdlApply apply, Schema schema) {
    Schema.Field parametersField = schema.getField(SapODataConstants.ApplyExpression.PARAMETERS_FIELD_NAME);
    Schema parametersSchema = parametersField.getSchema().isNullable()
      ? parametersField.getSchema().getNonNullable() : parametersField.getSchema();

    List<CsdlExpression> parameters = apply.getParameters();
    StructuredRecord.Builder parametersRecordBuilder = StructuredRecord.builder(parametersSchema);
    for (int i = 0; i < parameters.size(); i++) {
      CsdlExpression parameter = parameters.get(i);
      String parameterName = String.format("%s_%d", ODataUtil.typeOf(parameter), i);
      Schema.Field parameterField = parametersSchema.getField(parameterName);
      Schema parameterSchema = parameterField.getSchema().isNullable()
        ? parameterField.getSchema().getNonNullable() : parameterField.getSchema();

      parametersRecordBuilder.set(parameterName, extractExpression(fieldName, parameter, parameterSchema));
    }

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.ApplyExpression.NAME_FIELD_NAME, EdmExpression.EdmExpressionType.Apply.name())
      .set(SapODataConstants.ApplyExpression.FUNCTION_FIELD_NAME, apply.getFunction())
      .set(SapODataConstants.ApplyExpression.PARAMETERS_FIELD_NAME, parametersRecordBuilder.build())
      .build();
  }

  /**
   * Extract "Cast" expression record. OData 4 "Cast" expressions mapped to a record with field
   * "{@value SapODataConstants.CastIsOfExpression#NAME_FIELD_NAME}" for expression name,
   * "{@value SapODataConstants.CastIsOfExpression#TYPE_FIELD_NAME}" for a value of edm type name to cast,
   * "{@value SapODataConstants.CastIsOfExpression#MAX_LENGTH_FIELD_NAME}" for a maximum length of value,
   * "{@value SapODataConstants.CastIsOfExpression#PRECISION_FIELD_NAME}" for a precision of value,
   * "{@value SapODataConstants.CastIsOfExpression#SCALE_FIELD_NAME}" for a scale of value,
   * "{@value SapODataConstants.CastIsOfExpression#SRID_FIELD_NAME}" for a SRID of value.
   * "{@value SapODataConstants.CastIsOfExpression#VALUE_FIELD_NAME}" for a value expression.
   * <br/>
   * For example, for annotation with 'Cast' expression:
   * <p>
   * {@code
   * <Annotation Term="Core.Description">
   * <Cast Type="Edm.String">
   * <Path>SomeProperty</Path>
   * </Cast>
   * </Annotation>
   * }
   * </p>
   * expression will be mapped to the following {@link StructuredRecord}:
   * <pre>
   *   name: "Cast"
   *   type: "Edm.String"
   *   maxLength: null
   *   precision: null
   *   scale: null
   *   srid: null
   *   value:
   *    name: "Path"
   *    value: "SomeProperty"
   * </pre>
   *
   * @param fieldName annotation field name.
   * @param cast      "Cast" expression.
   * @param schema    "Cast" expression record schema.
   * @return expression record that corresponds to the given "Cast" expression.
   */
  private StructuredRecord extractCastExpression(String fieldName, CsdlCast cast, Schema schema) {
    Schema.Field valueField = schema.getField(SapODataConstants.CastIsOfExpression.VALUE_FIELD_NAME);
    Schema valueSchema = valueField.getSchema().isNullable() ? valueField.getSchema().getNonNullable()
      : valueField.getSchema();
    StructuredRecord valueRecord = extractExpression(fieldName, cast.getValue(), valueSchema);

    String maxLength = cast.getMaxLength() != null ? cast.getMaxLength().toString() : null;
    String precision = cast.getPrecision() != null ? cast.getPrecision().toString() : null;
    String scale = cast.getScale() != null ? cast.getScale().toString() : null;
    String srid = cast.getSrid() != null ? cast.getSrid().toString() : null;

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.CastIsOfExpression.NAME_FIELD_NAME, EdmExpression.EdmExpressionType.Cast.name())
      .set(SapODataConstants.CastIsOfExpression.TYPE_FIELD_NAME, cast.getType())
      .set(SapODataConstants.CastIsOfExpression.MAX_LENGTH_FIELD_NAME, maxLength)
      .set(SapODataConstants.CastIsOfExpression.PRECISION_FIELD_NAME, precision)
      .set(SapODataConstants.CastIsOfExpression.SCALE_FIELD_NAME, scale)
      .set(SapODataConstants.CastIsOfExpression.SRID_FIELD_NAME, srid)
      .set(SapODataConstants.CastIsOfExpression.VALUE_FIELD_NAME, valueRecord)
      .build();
  }

  /**
   * Extract "Collection" expression record. OData 4 "Collection" expression mapped to a record with field
   * "{@value SapODataConstants.CollectionExpression#NAME_FIELD_NAME}" for expression name,
   * "{@value SapODataConstants.CollectionExpression#ITEMS_FIELD_NAME}" for an array of item expressions.
   * <br/>
   * For example, for annotation with 'Collection' expression:
   * <p>
   * {@code
   * <Annotation Term="Core.Description">
   * <Collection>
   * <String>Product</String>
   * <String>Supplier</String>
   * <String>Customer</String>
   * </Collection>
   * </Annotation>
   * }
   * </p>
   * expression will be mapped to the following {@link StructuredRecord}:
   * <pre>
   *   name: "Collection"
   *   items: [
   *      name: "String"
   *      value: "Product",
   *
   *      name: "String"
   *      value: "Supplier",
   *
   *      name: "String"
   *      value: "Customer",
   *   ]
   * </pre>
   *
   * @param fieldName  annotation field name.
   * @param collection "Collection" expression.
   * @param schema     "Collection" expression record schema.
   * @return expression record that corresponds to the given "Collection" expression.
   */
  private StructuredRecord extractCollectionExpression(String fieldName, CsdlCollection collection, Schema schema) {
    String expressionName = EdmExpression.EdmExpressionType.Collection.name();
    Schema.Field itemsField = schema.getField(SapODataConstants.CollectionExpression.ITEMS_FIELD_NAME);
    if (itemsField == null) {
      return StructuredRecord.builder(schema)
        .set(SapODataConstants.CollectionExpression.NAME_FIELD_NAME, expressionName)
        .build();
    }

    Schema itemsSchema = itemsField.getSchema();
    Schema componentSchema = itemsSchema.getComponentSchema().isNullable()
      ? itemsSchema.getComponentSchema().getNonNullable() : itemsSchema.getComponentSchema();
    List<StructuredRecord> items = collection.getItems().stream()
      .map(i -> extractExpression(fieldName, i, componentSchema))
      .collect(Collectors.toList());

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.CollectionExpression.NAME_FIELD_NAME, expressionName)
      .set(SapODataConstants.CollectionExpression.ITEMS_FIELD_NAME, items)
      .build();
  }

  /**
   * Extract "If" expression record. OData 4 "If" expression mapped to a record with field
   * "{@value SapODataConstants.IfExpression#NAME_FIELD_NAME}" for expression name,
   * "{@value SapODataConstants.IfExpression#GUARD_FIELD_NAME}" for a guard expression,
   * "{@value SapODataConstants.IfExpression#THEN_FIELD_NAME}" for a then expression,
   * "{@value SapODataConstants.IfExpression#ELSE_FIELD_NAME}" for an else expression.
   * <br/>
   * For example, for annotation with 'If' expression:
   * <p>
   * {@code
   * <Annotation Term="Core.Description">
   * <If>
   * <Path>SomeBooleanProperty</Path>
   * <String>Female</String>
   * <String>Male</String>
   * </If>
   * </Annotation>
   * }
   * </p>
   * expression will be mapped to the following {@link StructuredRecord}:
   * <pre>
   *   name: "If"
   *   guard:
   *    name: "Path"
   *    value: "SomeBooleanProperty"
   *   then:
   *    name: "String"
   *    value: "Female"
   *   else:
   *    name: "String"
   *    value: "Male"
   * </pre>
   *
   * @param fieldName annotation field name.
   * @param edmIf     "If" expression.
   * @param schema    "If" expression record schema.
   * @return expression record that corresponds to the given "If" expression.
   */
  private StructuredRecord extractIfExpression(String fieldName, CsdlIf edmIf, Schema schema) {
    Schema.Field guardField = schema.getField(SapODataConstants.IfExpression.GUARD_FIELD_NAME);
    Schema guardSchema = guardField.getSchema().isNullable() ? guardField.getSchema().getNonNullable()
      : guardField.getSchema();

    StructuredRecord guardRecord = extractExpression(fieldName, edmIf.getGuard(), guardSchema);
    Schema.Field thenField = schema.getField(SapODataConstants.IfExpression.THEN_FIELD_NAME);
    Schema thenSchema = thenField.getSchema().isNullable() ? thenField.getSchema().getNonNullable()
      : thenField.getSchema();
    StructuredRecord thenRecord = extractExpression(fieldName, edmIf.getThen(), thenSchema);

    Schema.Field elseField = schema.getField(SapODataConstants.IfExpression.ELSE_FIELD_NAME);
    Schema elseSchema = elseField.getSchema().isNullable() ? elseField.getSchema().getNonNullable()
      : elseField.getSchema();
    StructuredRecord elseRecord = extractExpression(fieldName, edmIf.getElse(), elseSchema);

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.IfExpression.NAME_FIELD_NAME, EdmExpression.EdmExpressionType.If.name())
      .set(SapODataConstants.IfExpression.GUARD_FIELD_NAME, guardRecord)
      .set(SapODataConstants.IfExpression.THEN_FIELD_NAME, thenRecord)
      .set(SapODataConstants.IfExpression.ELSE_FIELD_NAME, elseRecord)
      .build();
  }

  /**
   * Extract "IsOf" expression record. OData 4 "IsOf" expressions mapped to a record with field
   * "{@value SapODataConstants.CastIsOfExpression#TYPE_FIELD_NAME}" for expression name,
   * "{@value SapODataConstants.CastIsOfExpression#TYPE_FIELD_NAME}" for a value of edm type name to check,
   * "{@value SapODataConstants.CastIsOfExpression#MAX_LENGTH_FIELD_NAME}" for a maximum length of value,
   * "{@value SapODataConstants.CastIsOfExpression#PRECISION_FIELD_NAME}" for a precision of value,
   * "{@value SapODataConstants.CastIsOfExpression#SCALE_FIELD_NAME}" for a scale of value,
   * "{@value SapODataConstants.CastIsOfExpression#SRID_FIELD_NAME}" for a SRID of value.
   * "{@value SapODataConstants.CastIsOfExpression#VALUE_FIELD_NAME}" for a value expression.
   * <br/>
   * For example, for annotation with 'IsOf' expression:
   * <p>
   * {@code
   * <Annotation Term="Core.Description">
   * <IsOf Type="Edm.Boolean">
   * <Path>SomeProperty</Path>
   * </IsOf>
   * </Annotation>
   * }
   * </p>
   * expression will be mapped to the following {@link StructuredRecord}:
   * <pre>
   *   name: "IsOf"
   *   type: "Edm.Boolean"
   *   maxLength: null
   *   precision: null
   *   scale: null
   *   srid: null
   *   value:
   *    name: "Path"
   *    value: "SomeProperty"
   * </pre>
   *
   * @param fieldName annotation field name.
   * @param isOf      "IsOf" expression.
   * @param schema    "IsOf" expression record schema.
   * @return expression record that corresponds to the given "IsOf" expression.
   */
  private StructuredRecord extractIsOfExpression(String fieldName, CsdlIsOf isOf, Schema schema) {
    Schema.Field valueField = schema.getField(SapODataConstants.CastIsOfExpression.VALUE_FIELD_NAME);
    Schema valueSchema = valueField.getSchema().isNullable() ? valueField.getSchema().getNonNullable()
      : valueField.getSchema();
    StructuredRecord valueRecord = extractExpression(fieldName, isOf.getValue(), valueSchema);

    String maxLength = isOf.getMaxLength() != null ? isOf.getMaxLength().toString() : null;
    String precision = isOf.getPrecision() != null ? isOf.getPrecision().toString() : null;
    String scale = isOf.getScale() != null ? isOf.getScale().toString() : null;
    String srid = isOf.getSrid() != null ? isOf.getSrid().toString() : null;

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.CastIsOfExpression.NAME_FIELD_NAME, EdmExpression.EdmExpressionType.IsOf.name())
      .set(SapODataConstants.CastIsOfExpression.TYPE_FIELD_NAME, isOf.getType())
      .set(SapODataConstants.CastIsOfExpression.MAX_LENGTH_FIELD_NAME, maxLength)
      .set(SapODataConstants.CastIsOfExpression.PRECISION_FIELD_NAME, precision)
      .set(SapODataConstants.CastIsOfExpression.SCALE_FIELD_NAME, scale)
      .set(SapODataConstants.CastIsOfExpression.SRID_FIELD_NAME, srid)
      .set(SapODataConstants.CastIsOfExpression.VALUE_FIELD_NAME, valueRecord)
      .build();
  }

  /**
   * Extract "LabeledElement" expression record. OData 4 "LabeledElement" expression mapped to CDAP record with field
   * "{@value SapODataConstants.LabeledElementExpression#ELEMENT_NAME_FIELD_NAME}" for expression name,
   * "{@value SapODataConstants.LabeledElementExpression#ELEMENT_NAME_FIELD_NAME}" for an element name.
   * "{@value SapODataConstants.LabeledElementExpression#VALUE_FIELD_NAME}" for expression value.
   * <br/>
   * For example, for annotation with 'LabeledElement' expression:
   * <p>
   * {@code
   * <Annotation Term="Core.Description">
   * <LabeledElement Name="CustomerFirstName">
   * <Path>SomeProperty</Path>
   * </LabeledElement>
   * </Annotation>
   * }
   * </p>
   * expression will be mapped to the following {@link StructuredRecord}:
   * <pre>
   *   name: "LabeledElement"
   *   elementName: "CustomerFirstName"
   *   value:
   *    name: "Path"
   *    value: "SomeProperty"
   * </pre>
   *
   * @param fieldName      annotation field name.
   * @param labeledElement "LabeledElement" expression.
   * @param schema         "LabeledElement" expression record schema.
   * @return expression record that corresponds to the given "LabeledElement" expression.
   */
  private StructuredRecord extractLabeledElementExpression(String fieldName, CsdlLabeledElement labeledElement,
                                                           Schema schema) {
    Schema.Field valueField = schema.getField(SapODataConstants.LabeledElementExpression.VALUE_FIELD_NAME);
    Schema valueSchema = valueField.getSchema().isNullable() ? valueField.getSchema().getNonNullable()
      : valueField.getSchema();
    StructuredRecord valueRecord = extractExpression(fieldName, labeledElement.getValue(), valueSchema);
    String expressionName = EdmExpression.EdmExpressionType.LabeledElement.name();

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.LabeledElementExpression.NAME_FIELD_NAME, expressionName)
      .set(SapODataConstants.LabeledElementExpression.ELEMENT_NAME_FIELD_NAME, labeledElement.getName())
      .set(SapODataConstants.LabeledElementExpression.VALUE_FIELD_NAME, valueRecord)
      .build();
  }

  /**
   * Extract "UrlRef" expression record. OData 4 "UrlRef" expression mapped to a record with field
   * "{@value SapODataConstants.UrlRefExpression#NAME_FIELD_NAME}" for expression name,
   * "{@value SapODataConstants.UrlRefExpression#VALUE_FIELD_NAME}" for value expression.
   * <br/>
   * For example, for annotation with 'UrlRef' expression:
   * <p>
   * {@code
   * <Annotation Term="Core.LongDescription">
   * <UrlRef><String>http://host/wiki/HowToUse</String></UrlRef>
   * </Annotation>
   * }
   * </p>
   * expression will be mapped to the following {@link StructuredRecord}:
   * <pre>
   *   name: "UrlRef"
   *   value:
   *    name: "String"
   *    value: "http://host/wiki/HowToUse"
   * </pre>
   *
   * @param fieldName annotation field name.
   * @param urlRef    "UrlRef" expression.
   * @param schema    "UrlRef" expression record schema.
   * @return expression record that corresponds to the given "UrlRef" expression.
   */
  private StructuredRecord extractUrlRefExpression(String fieldName, CsdlUrlRef urlRef, Schema schema) {
    Schema.Field valueField = schema.getField(SapODataConstants.UrlRefExpression.VALUE_FIELD_NAME);
    Schema valueSchema = valueField.getSchema().isNullable() ? valueField.getSchema().getNonNullable()
      : valueField.getSchema();
    StructuredRecord valueRecord = extractExpression(fieldName, urlRef.getValue(), valueSchema);

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.UrlRefExpression.NAME_FIELD_NAME, EdmExpression.EdmExpressionType.UrlRef.name())
      .set(SapODataConstants.UrlRefExpression.VALUE_FIELD_NAME, valueRecord)
      .build();
  }

  /**
   * Extract "Record" expression record. OData 4 "Record" expression mapped to a record with field
   * "{@value SapODataConstants.RecordExpression#NAME_FIELD_NAME}" for expression name,
   * "{@value SapODataConstants.RecordExpression#TYPE_FIELD_NAME}" for the qualified name of a structured type in scope,
   * "{@value SapODataConstants.RecordExpression#PROPERTY_VALUES_FIELD_NAME}" for the property values record,
   * "{@value SapODataConstants.RecordExpression#ANNOTATIONS_FIELD_NAME}" for nested annotations record.
   * <br/>
   * For example, for annotation with 'UrlRef' expression:
   * <p>
   * {@code
   * <Annotation Term="Core.Description">
   * <Record>
   * <Annotation Term="Core.Description" String="Annotation on record" />
   * <PropertyValue Property="GivenName" Path="SomeProperty1"/>
   * <PropertyValue Property="Age" Path="SomeProperty2"/>
   * </Record>
   * </Annotation>
   * }
   * </p>
   * expression will be mapped to the following {@link StructuredRecord}:
   * <pre>
   *   name: "Record"
   *   type: null
   *   propertyValues:
   *    GivenName:
   *      name: "Path"
   *      value: "SomeProperty1"
   *    Age:
   *      name: "Path"
   *      value: "SomeProperty2"
   *   annotations:
   *    core_description:
   *      term: "Core.Description"
   *      qualifier: null
   *      expression:
   *        name: "String"
   *        value: "Annotation on record"
   * </pre>
   *
   * @param record "Record" expression.
   * @param schema "Record" expression record schema.
   * @return expression record that corresponds to the given "Record" expression.
   */
  private StructuredRecord extractRecordExpression(CsdlRecord record, Schema schema) {
    List<CsdlPropertyValue> propertyValues = record.getPropertyValues();
    String type = record.getType();
    StructuredRecord.Builder builder = StructuredRecord.builder(schema)
      .set(SapODataConstants.RecordExpression.NAME_FIELD_NAME, EdmExpression.EdmExpressionType.Record.name())
      .set(SapODataConstants.RecordExpression.TYPE_FIELD_NAME, type);

    Schema.Field propertyValuesField = schema.getField(SapODataConstants.RecordExpression.PROPERTY_VALUES_FIELD_NAME);
    if (propertyValuesField != null && propertyValues != null && !propertyValues.isEmpty()) {
      Schema propertyValuesSchema = propertyValuesField.getSchema().isNullable()
        ? propertyValuesField.getSchema().getNonNullable() : propertyValuesField.getSchema();
      StructuredRecord propertyValuesRecord = extractRecordPropertyValues(propertyValues, propertyValuesSchema);
      builder.set(SapODataConstants.RecordExpression.PROPERTY_VALUES_FIELD_NAME, propertyValuesRecord);
    }

    Schema.Field nestedAnnotationsField = schema.getField(SapODataConstants.RecordExpression.ANNOTATIONS_FIELD_NAME);
    if (nestedAnnotationsField != null && record.getAnnotations() != null && !record.getAnnotations().isEmpty()) {
      Schema annotationsSchema = nestedAnnotationsField.getSchema().isNullable()
        ? nestedAnnotationsField.getSchema().getNonNullable() : nestedAnnotationsField.getSchema();
      Map<String, OData4Annotation> nestedAnnotationsByName = record.getAnnotations().stream()
        .map(OData4Annotation::new)
        .collect(Collectors.toMap(OData4Annotation::getName, Function.identity()));
      StructuredRecord annotationsRecord = extractNestedAnnotations(nestedAnnotationsByName, annotationsSchema);
      builder.set(SapODataConstants.RecordExpression.ANNOTATIONS_FIELD_NAME, annotationsRecord);
    }

    return builder.build();
  }

  private StructuredRecord extractRecordPropertyValues(List<CsdlPropertyValue> propertyValues, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    propertyValues.stream()
      .filter(pv -> Objects.nonNull(schema.getField(pv.getProperty())))
      .forEach(pv -> {
        String propertyName = pv.getProperty();
        Schema.Field field = schema.getField(propertyName);
        Schema propertySchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
        StructuredRecord propertyRecord = extractExpression(propertyName, pv.getValue(), propertySchema);
        builder.set(propertyName, propertyRecord);
      });

    return builder.build();
  }
}
