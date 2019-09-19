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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.sap.SapODataConstants;
import io.cdap.plugin.sap.odata.ODataAnnotation;
import io.cdap.plugin.sap.odata.ODataEntity;
import io.cdap.plugin.sap.odata.ODataUtil;
import io.cdap.plugin.sap.odata.odata2.OData2Annotation;
import io.cdap.plugin.sap.odata.odata4.OData4Annotation;
import org.apache.olingo.commons.api.edm.annotation.EdmExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlApply;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlCast;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlCollection;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlDynamicExpression;
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
import java.util.stream.Collectors;


/**
 * Transforms {@link ODataEntity} to {@link StructuredRecord} including metadata annotations.
 */
public class ODataEntryToRecordWithMetadataTransformer extends ODataEntryToRecordTransformer {

  private Map<String, List<ODataAnnotation>> fieldNameToAnnotations;

  public ODataEntryToRecordWithMetadataTransformer(Schema schema,
                                                   Map<String, List<ODataAnnotation>> fieldNameToAnnotations) {
    super(schema);
    this.fieldNameToAnnotations = fieldNameToAnnotations;
  }

  /**
   * Transforms given {@link ODataEntity} to {@link StructuredRecord} including metadata annotations.
   *
   * @param oDataEntity ODataEntry to be transformed.
   * @return {@link StructuredRecord} with metadata annotations that corresponds to the given {@link ODataEntity}.
   */
  public StructuredRecord transform(ODataEntity oDataEntity) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      Schema nonNullableSchema = field.getSchema().isNullable() ?
        field.getSchema().getNonNullable() : field.getSchema();
      String fieldName = field.getName();
      Object property = oDataEntity.getProperties().get(fieldName);
      List<ODataAnnotation> annotations = fieldNameToAnnotations.get(fieldName);
      Object extractedValue = annotations == null || annotations.isEmpty()
        ? extractValue(fieldName, property, nonNullableSchema)
        : extractValueMetadataRecord(fieldName, property, nonNullableSchema);
      builder.set(fieldName, extractedValue);
    }
    return builder.build();
  }

  private StructuredRecord extractValueMetadataRecord(String fieldName, Object value, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    Schema.Field valueField = schema.getField(SapODataConstants.VALUE_FIELD_NAME);
    Schema valueNonNullableSchema = valueField.getSchema().isNullable()
      ? valueField.getSchema().getNonNullable() : valueField.getSchema();
    builder.set(SapODataConstants.VALUE_FIELD_NAME, extractValue(fieldName, value, valueNonNullableSchema));

    Schema.Field metadataField = schema.getField(SapODataConstants.METADATA_ANNOTATIONS_FIELD_NAME);
    Schema metadataNonNullableSchema = metadataField.getSchema().isNullable()
      ? metadataField.getSchema().getNonNullable() : metadataField.getSchema();
    StructuredRecord metadataRecord = extractMetadataRecord(fieldName, metadataNonNullableSchema);
    builder.set(SapODataConstants.METADATA_ANNOTATIONS_FIELD_NAME, metadataRecord);

    return builder.build();
  }

  private StructuredRecord extractMetadataRecord(String fieldName, Schema schema) {
    List<ODataAnnotation> annotations = fieldNameToAnnotations.get(fieldName);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    annotations.stream()
      .filter(a -> Objects.nonNull(schema.getField(a.getName())))
      .forEach(a -> builder.set(a.getName(), extractAnnotationValue(a, fieldName, schema)));
    return builder.build();
  }

  private Object extractAnnotationValue(ODataAnnotation annotation, String fieldName, Schema schema) {
    if (annotation instanceof OData2Annotation) {
      return ((OData2Annotation) annotation).getValue();
    }
    // OData 4 annotations mapped to record
    Schema.Field annotationField = schema.getField(annotation.getName());
    Schema annotationRecordSchema = annotationField.getSchema().isNullable()
      ? annotationField.getSchema().getNonNullable() : annotationField.getSchema();
    return extractAnnotationRecord(fieldName, (OData4Annotation) annotation, annotationRecordSchema);
  }

  private StructuredRecord extractAnnotationRecord(String fieldName, OData4Annotation annotation, Schema schema) {
    Schema.Field expressionField = schema.getField(SapODataConstants.ANNOTATION_EXPRESSION_FIELD_NAME);
    Schema expressionSchema = expressionField.getSchema().isNullable() ? expressionField.getSchema().getNonNullable()
      : expressionField.getSchema();
    CsdlExpression expression = annotation.getExpression();
    StructuredRecord expressionRecord = extractExpressionRecord(fieldName, expression, expressionSchema);
    return StructuredRecord.builder(schema)
      .set(SapODataConstants.ANNOTATION_TERM_FIELD_NAME, annotation.getTerm())
      .set(SapODataConstants.ANNOTATION_QUALIFIER_FIELD_NAME, annotation.getQualifier())
      .set(SapODataConstants.ANNOTATION_EXPRESSION_FIELD_NAME, expressionRecord)
      .build();
  }

  private StructuredRecord extractExpressionRecord(String fieldName, CsdlExpression expression, Schema schema) {
    if (expression.isConstant()) {
      String constantExpressionValue = expression.asConstant().getValue();
      String typeName = expression.asConstant().getType().name();
      return extractSingleValueExpressionRecord(typeName, constantExpressionValue, schema);
    }
    CsdlDynamicExpression dynamic = expression.asDynamic();
    if (dynamic.isPath()) {
      String value = dynamic.asPath().getValue();
      return extractSingleValueExpressionRecord(EdmExpression.EdmExpressionType.Path.name(), value, schema);
    }
    if (dynamic.isAnnotationPath()) {
      String value = dynamic.asAnnotationPath().getValue();
      return extractSingleValueExpressionRecord(EdmExpression.EdmExpressionType.AnnotationPath.name(), value, schema);
    }
    if (dynamic.isLabeledElementReference()) {
      String value = dynamic.asLabeledElementReference().getValue();
      return extractSingleValueExpressionRecord(EdmExpression.EdmExpressionType.LabeledElementReference.name(), value,
                                                schema);
    }
    if (dynamic.isNavigationPropertyPath()) {
      String value = dynamic.asNavigationPropertyPath().getValue();
      return extractSingleValueExpressionRecord(EdmExpression.EdmExpressionType.NavigationPropertyPath.name(), value,
                                                schema);
    }
    if (dynamic.isPropertyPath()) {
      String value = dynamic.asPropertyPath().getValue();
      return extractSingleValueExpressionRecord(EdmExpression.EdmExpressionType.PropertyPath.name(), value, schema);
    }
    if (dynamic.isNull()) {
      return extractSingleValueExpressionRecord(EdmExpression.EdmExpressionType.Null.name(), null, schema);
    }
    if (dynamic.isApply()) {
      return extractApplyExpressionRecord(fieldName, expression.asDynamic().asApply(), schema);
    }
    if (dynamic.isCast()) {
      return extractCastExpressionRecord(fieldName, expression.asDynamic().asCast(), schema);
    }
    if (dynamic.isCollection()) {
      return extractCollectionExpressionRecord(fieldName, expression.asDynamic().asCollection(), schema);
    }
    if (dynamic.isIf()) {
      return extractIfExpressionRecord(fieldName, expression.asDynamic().asIf(), schema);
    }
    if (dynamic.isIsOf()) {
      return extractIsOfExpressionRecord(fieldName, expression.asDynamic().asIsOf(), schema);
    }
    if (dynamic.isLabeledElement()) {
      return extractLabeledElementExpressionRecord(fieldName, expression.asDynamic().asLabeledElement(), schema);
    }
    if (dynamic.isRecord()) {
      return extractRecordExpressionRecord(expression.asDynamic().asRecord(), schema);
    }
    if (dynamic.isUrlRef()) {
      return extractUrlRefExpressionRecord(fieldName, expression.asDynamic().asUrlRef(), schema);
    }
    // Expression can only be a logical at this point
    CsdlLogicalOrComparisonExpression logical = dynamic.asLogicalOrComparison();
    if (logical.getType() == CsdlLogicalOrComparisonExpression.LogicalOrComparisonExpressionType.Not) {
      // Negation expressions are represented as an element edm:Not that MUST contain a single annotation expression.
      // See 'Comparison and Logical Operators' section of
      // https://docs.oasis-open.org/odata/odata-csdl-xml/v4.01/csprd05/odata-csdl-xml-v4.01-csprd05.html
      // However, Olingo's EdmNot interface extends common interface for logical or comparison expressions.
      // Thus, value expression can be accessed via either 'getLeftExpression' or 'getRightExpression'.
      // See: AbstractEdmLogicalOrComparisonExpression#getRightExpression implementation for details
      CsdlExpression value = logical.getLeft();
      return extractNotExpressionRecord(fieldName, value, schema);
    }

    CsdlExpression andLeft = logical.getLeft();
    CsdlExpression andRight = logical.getRight();
    return extractLogicalExpressionRecord(fieldName, logical, andLeft, andRight, schema);
  }

  private StructuredRecord extractLogicalExpressionRecord(String fieldName,
                                                          CsdlLogicalOrComparisonExpression expression,
                                                          CsdlExpression left, CsdlExpression right, Schema schema) {
    Schema leftFieldSchema = schema.getField(SapODataConstants.EXPRESSION_LEFT_FIELD_NAME).getSchema();
    Schema rightFieldSchema = schema.getField(SapODataConstants.EXPRESSION_RIGHT_FIELD_NAME).getSchema();
    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, expression.getType().name())
      .set(SapODataConstants.EXPRESSION_LEFT_FIELD_NAME, extractExpressionRecord(fieldName, left, leftFieldSchema))
      .set(SapODataConstants.EXPRESSION_RIGHT_FIELD_NAME, extractExpressionRecord(fieldName, right, rightFieldSchema))
      .build();
  }

  private StructuredRecord extractNotExpressionRecord(String fieldName, CsdlExpression value, Schema schema) {
    Schema leftFieldSchema = schema.getField(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME).getSchema();
    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, EdmExpression.EdmExpressionType.Not.name())
      .set(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, extractExpressionRecord(fieldName, value, leftFieldSchema))
      .build();
  }

  private StructuredRecord extractSingleValueExpressionRecord(String expressionName, Object value, Schema schema) {
    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, expressionName)
      .set(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, value)
      .build();
  }

  private StructuredRecord extractApplyExpressionRecord(String fieldName, CsdlApply apply, Schema schema) {
    Schema.Field parametersField = schema.getField(SapODataConstants.EXPRESSION_PARAMETERS_FIELD_NAME);
    Schema parametersSchema = parametersField.getSchema().isNullable()
      ? parametersField.getSchema().getNonNullable() : parametersField.getSchema();

    // TODO avoid duplication with Source
    List<CsdlExpression> parameters = apply.getParameters();
    StructuredRecord.Builder builder = StructuredRecord.builder(parametersSchema);
    for (int i = 0; i < parameters.size(); i++) {
      CsdlExpression parameter = parameters.get(i);
      String parameterName = String.format("%d-%s", i, ODataUtil.typeOf(parameter));
      Schema.Field parameterField = parametersSchema.getField(parameterName);
      Schema parameterSchema = parameterField.getSchema().isNullable()
        ? parameterField.getSchema().getNonNullable() : parameterField.getSchema();

      builder.set(parameterName, extractExpressionRecord(fieldName, parameter, parameterSchema));
    }

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, EdmExpression.EdmExpressionType.Apply.name())
      .set(SapODataConstants.EXPRESSION_FUNCTION_FIELD_NAME, apply.getFunction())
      .set(SapODataConstants.EXPRESSION_PARAMETERS_FIELD_NAME, builder.build())
      .build();
  }

  private StructuredRecord extractCastExpressionRecord(String fieldName, CsdlCast cast, Schema schema) {
    Schema.Field valueField = schema.getField(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME);
    Schema valueSchema = valueField.getSchema().isNullable() ? valueField.getSchema().getNonNullable()
      : valueField.getSchema();
    StructuredRecord valueRecord = extractExpressionRecord(fieldName, cast.getValue(), valueSchema);

    String maxLength = cast.getMaxLength() != null ? cast.getMaxLength().toString() : null;
    String precision = cast.getPrecision() != null ? cast.getPrecision().toString() : null;
    String scale = cast.getScale() != null ? cast.getScale().toString() : null;
    String srid = cast.getSrid() != null ? cast.getSrid().toString() : null;

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, EdmExpression.EdmExpressionType.Cast.name())
      .set(SapODataConstants.EXPRESSION_TYPE_FIELD_NAME, cast.getType())
      .set(SapODataConstants.EXPRESSION_MAX_LENGTH_FIELD_NAME, maxLength)
      .set(SapODataConstants.EXPRESSION_PRECISION_FIELD_NAME, precision)
      .set(SapODataConstants.EXPRESSION_SCALE_FIELD_NAME, scale)
      .set(SapODataConstants.EXPRESSION_SRID_FIELD_NAME, srid)
      .set(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, valueRecord)
      .build();
  }

  private StructuredRecord extractCollectionExpressionRecord(String fieldName, CsdlCollection collection,
                                                             Schema schema) {
    String expressionName = EdmExpression.EdmExpressionType.Collection.name();
    Schema.Field itemsField = schema.getField(SapODataConstants.EXPRESSION_ITEMS_FIELD_NAME);
    if (itemsField == null) {
      return StructuredRecord.builder(schema)
        .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, expressionName)
        .build();
    }

    Schema itemsSchema = itemsField.getSchema();
    Schema componentSchema = itemsSchema.getComponentSchema().isNullable()
      ? itemsSchema.getComponentSchema().getNonNullable() : itemsSchema.getComponentSchema();
    List<StructuredRecord> items = collection.getItems().stream()
      .map(i -> extractExpressionRecord(fieldName, i, componentSchema))
      .collect(Collectors.toList());

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, expressionName)
      .set(SapODataConstants.EXPRESSION_ITEMS_FIELD_NAME, items)
      .build();
  }

  private StructuredRecord extractIfExpressionRecord(String fieldName, CsdlIf edmIf, Schema schema) {
    Schema.Field guardField = schema.getField(SapODataConstants.EXPRESSION_GUARD_FIELD_NAME);
    Schema guardSchema = guardField.getSchema().isNullable() ? guardField.getSchema().getNonNullable()
      : guardField.getSchema();

    StructuredRecord guardRecord = extractExpressionRecord(fieldName, edmIf.getGuard(), guardSchema);
    Schema.Field thenField = schema.getField(SapODataConstants.EXPRESSION_THEN_FIELD_NAME);
    Schema thenSchema = thenField.getSchema().isNullable() ? thenField.getSchema().getNonNullable()
      : thenField.getSchema();
    StructuredRecord thenRecord = extractExpressionRecord(fieldName, edmIf.getThen(), thenSchema);

    Schema.Field elseField = schema.getField(SapODataConstants.EXPRESSION_ELSE_FIELD_NAME);
    Schema elseSchema = elseField.getSchema().isNullable() ? elseField.getSchema().getNonNullable()
      : elseField.getSchema();
    StructuredRecord elseRecord = extractExpressionRecord(fieldName, edmIf.getElse(), elseSchema);

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, EdmExpression.EdmExpressionType.If.name())
      .set(SapODataConstants.EXPRESSION_GUARD_FIELD_NAME, guardRecord)
      .set(SapODataConstants.EXPRESSION_THEN_FIELD_NAME, thenRecord)
      .set(SapODataConstants.EXPRESSION_ELSE_FIELD_NAME, elseRecord)
      .build();
  }

  private StructuredRecord extractIsOfExpressionRecord(String fieldName, CsdlIsOf isOf, Schema schema) {
    Schema.Field valueField = schema.getField(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME);
    Schema valueSchema = valueField.getSchema().isNullable() ? valueField.getSchema().getNonNullable()
      : valueField.getSchema();
    StructuredRecord valueRecord = extractExpressionRecord(fieldName, isOf.getValue(), valueSchema);

    String maxLength = isOf.getMaxLength() != null ? isOf.getMaxLength().toString() : null;
    String precision = isOf.getPrecision() != null ? isOf.getPrecision().toString() : null;
    String scale = isOf.getScale() != null ? isOf.getScale().toString() : null;
    String srid = isOf.getSrid() != null ? isOf.getSrid().toString() : null;

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, EdmExpression.EdmExpressionType.IsOf.name())
      .set(SapODataConstants.EXPRESSION_TYPE_FIELD_NAME, isOf.getType())
      .set(SapODataConstants.EXPRESSION_MAX_LENGTH_FIELD_NAME, maxLength)
      .set(SapODataConstants.EXPRESSION_PRECISION_FIELD_NAME, precision)
      .set(SapODataConstants.EXPRESSION_SCALE_FIELD_NAME, scale)
      .set(SapODataConstants.EXPRESSION_SRID_FIELD_NAME, srid)
      .set(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, valueRecord)
      .build();
  }

  private StructuredRecord extractLabeledElementExpressionRecord(String fieldName, CsdlLabeledElement labeledElement,
                                                                 Schema schema) {
    Schema.Field valueField = schema.getField(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME);
    Schema valueSchema = valueField.getSchema().isNullable() ? valueField.getSchema().getNonNullable()
      : valueField.getSchema();
    StructuredRecord valueRecord = extractExpressionRecord(fieldName, labeledElement.getValue(), valueSchema);

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, EdmExpression.EdmExpressionType.LabeledElement.name())
      .set(SapODataConstants.EXPRESSION_ELEMENT_NAME_FIELD_NAME, labeledElement.getName())
      .set(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, valueRecord)
      .build();
  }

  private StructuredRecord extractUrlRefExpressionRecord(String fieldName, CsdlUrlRef urlRef, Schema schema) {
    Schema.Field valueField = schema.getField(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME);
    Schema valueSchema = valueField.getSchema().isNullable() ? valueField.getSchema().getNonNullable()
      : valueField.getSchema();
    StructuredRecord valueRecord = extractExpressionRecord(fieldName, urlRef.getValue(), valueSchema);

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, EdmExpression.EdmExpressionType.UrlRef.name())
      .set(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, valueRecord)
      .build();
  }

  private StructuredRecord extractRecordExpressionRecord(CsdlRecord record, Schema schema) {
    Schema.Field propertyValuesField = schema.getField(SapODataConstants.EXPRESSION_PROPERTY_VALUES_FIELD_NAME);
    List<CsdlPropertyValue> propertyValues = record.getPropertyValues();
    String type = record.getType();
    if (propertyValuesField == null || propertyValues == null || propertyValues.isEmpty()) {
      return StructuredRecord.builder(schema)
        .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, EdmExpression.EdmExpressionType.Record.name())
        .set(SapODataConstants.EXPRESSION_TYPE_FIELD_NAME, type)
        .build();
    }
    Schema propertyValuesSchema = propertyValuesField.getSchema().isNullable()
      ? propertyValuesField.getSchema().getNonNullable() : propertyValuesField.getSchema();

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, EdmExpression.EdmExpressionType.Record.name())
      .set(SapODataConstants.EXPRESSION_TYPE_FIELD_NAME, type)
      .set(SapODataConstants.EXPRESSION_PROPERTY_VALUES_FIELD_NAME,
           extractRecordExpressionRecord(propertyValues, propertyValuesSchema))
      .build();
  }

  private StructuredRecord extractRecordExpressionRecord(List<CsdlPropertyValue> propertyValues, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    propertyValues.stream()
      .filter(pv -> Objects.nonNull(schema.getField(pv.getProperty())))
      .forEach(pv -> {
        String propertyName = pv.getProperty();
        Schema.Field field = schema.getField(propertyName);
        Schema propertySchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
        StructuredRecord propertyRecord = extractExpressionRecord(propertyName, pv.getValue(), propertySchema);
        builder.set(propertyName, propertyRecord);
      });

    return builder.build();
  }
}
