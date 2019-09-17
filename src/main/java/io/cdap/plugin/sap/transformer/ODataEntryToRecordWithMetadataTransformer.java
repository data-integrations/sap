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
import io.cdap.plugin.sap.odata.ODataAnnotation;
import io.cdap.plugin.sap.odata.ODataEntity;
import io.cdap.plugin.sap.odata.odata2.OData2Annotation;
import io.cdap.plugin.sap.odata.odata4.OData4Annotation;
import org.apache.olingo.commons.api.edm.EdmStructuredType;
import org.apache.olingo.commons.api.edm.annotation.EdmAnd;
import org.apache.olingo.commons.api.edm.annotation.EdmApply;
import org.apache.olingo.commons.api.edm.annotation.EdmCast;
import org.apache.olingo.commons.api.edm.annotation.EdmCollection;
import org.apache.olingo.commons.api.edm.annotation.EdmEq;
import org.apache.olingo.commons.api.edm.annotation.EdmExpression;
import org.apache.olingo.commons.api.edm.annotation.EdmGe;
import org.apache.olingo.commons.api.edm.annotation.EdmGt;
import org.apache.olingo.commons.api.edm.annotation.EdmIf;
import org.apache.olingo.commons.api.edm.annotation.EdmIsOf;
import org.apache.olingo.commons.api.edm.annotation.EdmLabeledElement;
import org.apache.olingo.commons.api.edm.annotation.EdmLe;
import org.apache.olingo.commons.api.edm.annotation.EdmLt;
import org.apache.olingo.commons.api.edm.annotation.EdmNe;
import org.apache.olingo.commons.api.edm.annotation.EdmNot;
import org.apache.olingo.commons.api.edm.annotation.EdmOr;
import org.apache.olingo.commons.api.edm.annotation.EdmPropertyValue;
import org.apache.olingo.commons.api.edm.annotation.EdmRecord;
import org.apache.olingo.commons.api.edm.annotation.EdmUrlRef;

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
    EdmExpression expression = annotation.getExpression();
    StructuredRecord expressionRecord = extractExpressionRecord(fieldName, expression, expressionSchema);
    return StructuredRecord.builder(schema)
      .set(SapODataConstants.ANNOTATION_TERM_FIELD_NAME, annotation.getTerm())
      .set(SapODataConstants.ANNOTATION_QUALIFIER_FIELD_NAME, annotation.getQualifier())
      .set(SapODataConstants.ANNOTATION_EXPRESSION_FIELD_NAME, expressionRecord)
      .build();
  }

  private StructuredRecord extractExpressionRecord(String fieldName, EdmExpression expression, Schema schema) {
    EdmExpression.EdmExpressionType type = expression.getExpressionType();
    switch (type) {
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
        String constantExpressionValue = expression.asConstant().getValueAsString();
        return extractSingleValueExpressionRecord(expression.getExpressionName(), constantExpressionValue, schema);
      case AnnotationPath:
        String annotationPathValue = expression.asDynamic().asAnnotationPath().getValue();
        return extractSingleValueExpressionRecord(expression.getExpressionName(), annotationPathValue, schema);
      case LabeledElementReference:
        String labeledElementReferenceValue = expression.asDynamic().asLabeledElementReference().getValue();
        return extractSingleValueExpressionRecord(expression.getExpressionName(), labeledElementReferenceValue, schema);
      case Null:
        return extractSingleValueExpressionRecord(expression.getExpressionName(), null, schema);
      case NavigationPropertyPath:
        String navigationPropertyPathValue = expression.asDynamic().asNavigationPropertyPath().getValue();
        return extractSingleValueExpressionRecord(expression.getExpressionName(), navigationPropertyPathValue, schema);
      case PropertyPath:
        String propertyPathValue = expression.asDynamic().asPropertyPath().getValue();
        return extractSingleValueExpressionRecord(expression.getExpressionName(), propertyPathValue, schema);
      case Path:
        String pathExpressionValue = expression.asDynamic().asPath().getValue();
        return extractSingleValueExpressionRecord(expression.getExpressionName(), pathExpressionValue, schema);
      case Apply:
        return extractApplyExpressionRecord(fieldName, expression.asDynamic().asApply(), schema);
      case And:
        EdmAnd and = expression.asDynamic().asAnd();
        EdmExpression andLeft = and.getLeftExpression();
        EdmExpression andRight = and.getRightExpression();
        return extractLogicalExpressionRecord(fieldName, and, andLeft, andRight, schema);
      case Or:
        EdmOr or = expression.asDynamic().asOr();
        EdmExpression orLeft = or.getLeftExpression();
        EdmExpression orRight = or.getRightExpression();
        return extractLogicalExpressionRecord(fieldName, or, orLeft, orRight, schema);
      case Not:
        EdmNot not = expression.asDynamic().asNot();
        EdmExpression value = not.getLeftExpression();
        return extractNotExpressionRecord(fieldName, not, value, schema);
      case Eq:
        EdmEq eq = expression.asDynamic().asEq();
        EdmExpression eqLeft = eq.getLeftExpression();
        EdmExpression eqRight = eq.getRightExpression();
        return extractLogicalExpressionRecord(fieldName, eq, eqLeft, eqRight, schema);
      case Ne:
        EdmNe ne = expression.asDynamic().asNe();
        EdmExpression neLeft = ne.getLeftExpression();
        EdmExpression neRight = ne.getRightExpression();
        return extractLogicalExpressionRecord(fieldName, ne, neLeft, neRight, schema);
      case Gt:
        EdmGt gt = expression.asDynamic().asGt();
        EdmExpression gtLeft = gt.getLeftExpression();
        EdmExpression gtRight = gt.getRightExpression();
        return extractLogicalExpressionRecord(fieldName, gt, gtLeft, gtRight, schema);
      case Ge:
        EdmGe ge = expression.asDynamic().asGe();
        EdmExpression geLeft = ge.getLeftExpression();
        EdmExpression geRight = ge.getRightExpression();
        return extractLogicalExpressionRecord(fieldName, ge, geLeft, geRight, schema);
      case Lt:
        EdmLt lt = expression.asDynamic().asLt();
        EdmExpression ltLeft = lt.getLeftExpression();
        EdmExpression ltRight = lt.getRightExpression();
        return extractLogicalExpressionRecord(fieldName, lt, ltLeft, ltRight, schema);
      case Le:
        EdmLe le = expression.asDynamic().asLe();
        EdmExpression leLeft = le.getLeftExpression();
        EdmExpression leRight = le.getRightExpression();
        return extractLogicalExpressionRecord(fieldName, le, leLeft, leRight, schema);
      case Cast:
        return extractCastExpressionRecord(fieldName, expression.asDynamic().asCast(), schema);
      case Collection:
        return extractCollectionExpressionRecord(fieldName, expression.asDynamic().asCollection(), schema);
      case If:
        return extractIfExpressionRecord(fieldName, expression.asDynamic().asIf(), schema);
      case IsOf:
        return extractIsOfExpressionRecord(fieldName, expression.asDynamic().asIsOf(), schema);
      case LabeledElement:
        return extractLabeledElementExpressionRecord(fieldName, expression.asDynamic().asLabeledElement(), schema);
      case Record:
        return extractRecordExpressionRecord(expression.asDynamic().asRecord(), schema);
      case UrlRef:
        return extractUrlRefExpressionRecord(fieldName, expression.asDynamic().asUrlRef(), schema);
      default:
        // this should never happen
        throw new UnexpectedFormatException(
          String.format("Annotation expression for field '%s' is of unsupported type '%s'.", fieldName, type));
    }
  }

  private StructuredRecord extractLogicalExpressionRecord(String fieldName, EdmExpression expression,
                                                          EdmExpression left, EdmExpression right, Schema schema) {
    Schema leftFieldSchema = schema.getField(SapODataConstants.EXPRESSION_LEFT_FIELD_NAME).getSchema();
    Schema rightFieldSchema = schema.getField(SapODataConstants.EXPRESSION_RIGHT_FIELD_NAME).getSchema();

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, expression.getExpressionName())
      .set(SapODataConstants.EXPRESSION_LEFT_FIELD_NAME, extractExpressionRecord(fieldName, left, leftFieldSchema))
      .set(SapODataConstants.EXPRESSION_RIGHT_FIELD_NAME, extractExpressionRecord(fieldName, right, rightFieldSchema))
      .build();
  }

  private StructuredRecord extractNotExpressionRecord(String fieldName, EdmExpression expression,
                                                      EdmExpression value, Schema schema) {
    Schema leftFieldSchema = schema.getField(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME).getSchema();
    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, expression.getExpressionName())
      .set(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, extractExpressionRecord(fieldName, value, leftFieldSchema))
      .build();
  }

  private StructuredRecord extractSingleValueExpressionRecord(String expressionName, Object value, Schema schema) {
    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, expressionName)
      .set(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, value)
      .build();
  }

  private StructuredRecord extractApplyExpressionRecord(String fieldName, EdmApply apply, Schema schema) {
    Schema.Field parametersField = schema.getField(SapODataConstants.EXPRESSION_PARAMETERS_FIELD_NAME);
    Schema parametersSchema = parametersField.getSchema().isNullable()
      ? parametersField.getSchema().getNonNullable() : parametersField.getSchema();

    StructuredRecord.Builder builder = StructuredRecord.builder(parametersSchema);

    for (EdmExpression parameterExpression : apply.getParameters()) {
      String name = parameterExpression.getExpressionName();
      Schema.Field parameterField = parametersSchema.getField(name);
      Schema parameterSchema = parameterField.getSchema().isNullable()
        ? parameterField.getSchema().getNonNullable() : parameterField.getSchema();
      builder.set(name, extractExpressionRecord(fieldName, parameterExpression, parameterSchema));
    }

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, apply.getExpressionName())
      .set(SapODataConstants.EXPRESSION_FUNCTION_FIELD_NAME, apply.getFunction())
      .set(SapODataConstants.EXPRESSION_PARAMETERS_FIELD_NAME, builder.build())
      .build();
  }

  private StructuredRecord extractCastExpressionRecord(String fieldName, EdmCast cast, Schema schema) {
    Schema.Field valueField = schema.getField(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME);
    Schema valueSchema = valueField.getSchema().isNullable() ? valueField.getSchema().getNonNullable()
      : valueField.getSchema();
    StructuredRecord valueRecord = extractExpressionRecord(fieldName, cast.getValue(), valueSchema);

    String type = cast.getType().getFullQualifiedName().getFullQualifiedNameAsString();
    String maxLength = cast.getMaxLength() != null ? cast.getMaxLength().toString() : null;
    String precision = cast.getPrecision() != null ? cast.getPrecision().toString() : null;
    String scale = cast.getScale() != null ? cast.getScale().toString() : null;
    String srid = cast.getSrid() != null ? cast.getSrid().toString() : null;

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, cast.getExpressionName())
      .set(SapODataConstants.EXPRESSION_TYPE_FIELD_NAME, type)
      .set(SapODataConstants.EXPRESSION_MAX_LENGTH_FIELD_NAME, maxLength)
      .set(SapODataConstants.EXPRESSION_PRECISION_FIELD_NAME, precision)
      .set(SapODataConstants.EXPRESSION_SCALE_FIELD_NAME, scale)
      .set(SapODataConstants.EXPRESSION_SRID_FIELD_NAME, srid)
      .set(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, valueRecord)
      .build();
  }

  private StructuredRecord extractCollectionExpressionRecord(String fieldName, EdmCollection collection,
                                                             Schema schema) {
    Schema.Field itemsField = schema.getField(SapODataConstants.EXPRESSION_ITEMS_FIELD_NAME);
    if (itemsField == null) {
      return StructuredRecord.builder(schema)
        .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, collection.getExpressionName())
        .build();
    }

    Schema itemsSchema = itemsField.getSchema();
    Schema componentSchema = itemsSchema.getComponentSchema().isNullable()
      ? itemsSchema.getComponentSchema().getNonNullable() : itemsSchema.getComponentSchema();
    List<StructuredRecord> items = collection.getItems().stream()
      .map(i -> extractExpressionRecord(fieldName, i, componentSchema))
      .collect(Collectors.toList());

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, collection.getExpressionName())
      .set(SapODataConstants.EXPRESSION_ITEMS_FIELD_NAME, items)
      .build();
  }

  private StructuredRecord extractIfExpressionRecord(String fieldName, EdmIf edmIf, Schema schema) {
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
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, edmIf.getExpressionName())
      .set(SapODataConstants.EXPRESSION_GUARD_FIELD_NAME, guardRecord)
      .set(SapODataConstants.EXPRESSION_THEN_FIELD_NAME, thenRecord)
      .set(SapODataConstants.EXPRESSION_ELSE_FIELD_NAME, elseRecord)
      .build();
  }

  private StructuredRecord extractIsOfExpressionRecord(String fieldName, EdmIsOf isOf, Schema schema) {
    Schema.Field valueField = schema.getField(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME);
    Schema valueSchema = valueField.getSchema().isNullable() ? valueField.getSchema().getNonNullable()
      : valueField.getSchema();
    StructuredRecord valueRecord = extractExpressionRecord(fieldName, isOf.getValue(), valueSchema);

    String type = isOf.getType().getFullQualifiedName().getFullQualifiedNameAsString();
    String maxLength = isOf.getMaxLength() != null ? isOf.getMaxLength().toString() : null;
    String precision = isOf.getPrecision() != null ? isOf.getPrecision().toString() : null;
    String scale = isOf.getScale() != null ? isOf.getScale().toString() : null;
    String srid = isOf.getSrid() != null ? isOf.getSrid().toString() : null;

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, isOf.getExpressionName())
      .set(SapODataConstants.EXPRESSION_TYPE_FIELD_NAME, type)
      .set(SapODataConstants.EXPRESSION_MAX_LENGTH_FIELD_NAME, maxLength)
      .set(SapODataConstants.EXPRESSION_PRECISION_FIELD_NAME, precision)
      .set(SapODataConstants.EXPRESSION_SCALE_FIELD_NAME, scale)
      .set(SapODataConstants.EXPRESSION_SRID_FIELD_NAME, srid)
      .set(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, valueRecord)
      .build();
  }

  private StructuredRecord extractLabeledElementExpressionRecord(String fieldName, EdmLabeledElement labeledElement,
                                                                 Schema schema) {
    Schema.Field valueField = schema.getField(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME);
    Schema valueSchema = valueField.getSchema().isNullable() ? valueField.getSchema().getNonNullable()
      : valueField.getSchema();
    StructuredRecord valueRecord = extractExpressionRecord(fieldName, labeledElement.getValue(), valueSchema);

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, labeledElement.getExpressionName())
      .set(SapODataConstants.EXPRESSION_ELEMENT_NAME_FIELD_NAME, labeledElement.getName())
      .set(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, valueRecord)
      .build();
  }

  private StructuredRecord extractUrlRefExpressionRecord(String fieldName, EdmUrlRef urlRef, Schema schema) {
    Schema.Field valueField = schema.getField(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME);
    Schema valueSchema = valueField.getSchema().isNullable() ? valueField.getSchema().getNonNullable()
      : valueField.getSchema();
    StructuredRecord valueRecord = extractExpressionRecord(fieldName, urlRef.getValue(), valueSchema);

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, urlRef.getExpressionName())
      .set(SapODataConstants.EXPRESSION_VALUE_FIELD_NAME, valueRecord)
      .build();
  }

  private StructuredRecord extractRecordExpressionRecord(EdmRecord record, Schema schema) {
    Schema.Field propertyValuesField = schema.getField(SapODataConstants.EXPRESSION_PROPERTY_VALUES_FIELD_NAME);
    List<EdmPropertyValue> propertyValues = record.getPropertyValues();
    EdmStructuredType recordType = record.getType();
    String type = recordType != null ? recordType.getFullQualifiedName().getFullQualifiedNameAsString() : null;
    if (propertyValuesField == null || propertyValues == null || propertyValues.isEmpty()) {
      return StructuredRecord.builder(schema)
        .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, record.getExpressionName())
        .set(SapODataConstants.EXPRESSION_TYPE_FIELD_NAME, type)
        .build();
    }

    Schema propertyValuesSchema = propertyValuesField.getSchema().isNullable()
      ? propertyValuesField.getSchema().getNonNullable() : propertyValuesField.getSchema();

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.EXPRESSION_NAME_FIELD_NAME, record.getExpressionName())
      .set(SapODataConstants.EXPRESSION_TYPE_FIELD_NAME, type)
      .set(SapODataConstants.EXPRESSION_PROPERTY_VALUES_FIELD_NAME,
           extractRecordExpressionRecord(propertyValues, propertyValuesSchema))
      .build();
  }

  private StructuredRecord extractRecordExpressionRecord(List<EdmPropertyValue> propertyValues, Schema schema) {
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
