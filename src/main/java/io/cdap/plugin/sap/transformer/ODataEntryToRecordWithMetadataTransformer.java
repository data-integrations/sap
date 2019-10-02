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
import io.cdap.plugin.sap.odata.ODataUtil;
import io.cdap.plugin.sap.odata.odata2.OData2Annotation;
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
      StructuredRecord expressionRecord = extractExpressionRecord(fieldName, expression, expressionSchema);
      builder.set(SapODataConstants.Annotation.EXPRESSION_FIELD_NAME, expressionRecord);
    }

    Map<String, OData4Annotation> nestedAnnotationsByName = annotation.getAnnotations();
    if (nestedAnnotationsByName.isEmpty()) {
      return builder.build();
    }

    Schema.Field nestedAnnotationsField = schema.getField(SapODataConstants.Annotation.ANNOTATIONS_FIELD_NAME);
    Schema annotationsSchema = nestedAnnotationsField.getSchema().isNullable()
      ? nestedAnnotationsField.getSchema().getNonNullable() : nestedAnnotationsField.getSchema();
    StructuredRecord annotationsRecord = extractNestedAnnotationsRecord(nestedAnnotationsByName, annotationsSchema);
    return builder
      .set(SapODataConstants.Annotation.ANNOTATIONS_FIELD_NAME, annotationsRecord)
      .build();
  }

  private StructuredRecord extractNestedAnnotationsRecord(Map<String, OData4Annotation> annotations, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    schema.getFields().forEach(f -> {
      Schema annotationSchema = f.getSchema().isNullable() ? f.getSchema().getNonNullable() : f.getSchema();
      OData4Annotation nestedAnnotation = annotations.get(f.getName());
      builder.set(f.getName(), extractAnnotationRecord(f.getName(), nestedAnnotation, annotationSchema));
    });

    return builder.build();
  }

  private StructuredRecord extractExpressionRecord(String fieldName, CsdlExpression expression, Schema schema) {
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
        return extractSingleValueExpressionRecord(typeName, constantExpressionValue, schema);
      case Path:
        String pathValue = expression.asDynamic().asPath().getValue();
        String pathType = EdmExpression.EdmExpressionType.Path.name();
        return extractSingleValueExpressionRecord(pathType, pathValue, schema);
      case AnnotationPath:
        String annotationPathValue = expression.asDynamic().asAnnotationPath().getValue();
        String annotationPathType = EdmExpression.EdmExpressionType.AnnotationPath.name();
        return extractSingleValueExpressionRecord(annotationPathType, annotationPathValue, schema);
      case LabeledElementReference:
        String labeledElementReferenceValue = expression.asDynamic().asLabeledElementReference().getValue();
        String labeledElementReferenceType = EdmExpression.EdmExpressionType.LabeledElementReference.name();
        return extractSingleValueExpressionRecord(labeledElementReferenceType, labeledElementReferenceValue, schema);
      case NavigationPropertyPath:
        String navigationPropertyPathValue = expression.asDynamic().asNavigationPropertyPath().getValue();
        String navigationPropertyPathType = EdmExpression.EdmExpressionType.NavigationPropertyPath.name();
        return extractSingleValueExpressionRecord(navigationPropertyPathType, navigationPropertyPathValue, schema);
      case PropertyPath:
        String propertyPathValue = expression.asDynamic().asPropertyPath().getValue();
        String propertyPathType = EdmExpression.EdmExpressionType.PropertyPath.name();
        return extractSingleValueExpressionRecord(propertyPathType, propertyPathValue, schema);
      case And:
      case Or:
      case Eq:
      case Ne:
      case Gt:
      case Ge:
      case Lt:
      case Le:
        // Annotations on logical & comparison expressionsare not included.
        // Olingo V4 client does not parse expression annotations correctly:
        // https://issues.apache.org/jira/browse/OLINGO-1403
        CsdlLogicalOrComparisonExpression logicalOrComparison = expression.asDynamic().asLogicalOrComparison();
        CsdlExpression andLeft = logicalOrComparison.getLeft();
        CsdlExpression andRight = logicalOrComparison.getRight();
        return extractLogicalExpressionRecord(fieldName, logicalOrComparison, andLeft, andRight, schema);
      case Not:
        // Negation expressions are represented as an element edm:Not that MUST contain a single annotation expression.
        // See 'Comparison and Logical Operators' section of
        // https://docs.oasis-open.org/odata/odata-csdl-xml/v4.01/csprd05/odata-csdl-xml-v4.01-csprd05.html
        // However, Olingo's EdmNot interface extends common interface for logical or comparison expressions.
        // Thus, value expression can be accessed via either 'getLeftExpression' or 'getRightExpression'.
        // See: AbstractEdmLogicalOrComparisonExpression#getRightExpression implementation for details
        CsdlExpression value = expression.asDynamic().asLogicalOrComparison().getLeft();
        return extractNotExpressionRecord(fieldName, value, schema);
      case Apply:
        return extractApplyExpressionRecord(fieldName, expression.asDynamic().asApply(), schema);
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

  private StructuredRecord extractLogicalExpressionRecord(String fieldName, CsdlLogicalOrComparisonExpression logical,
                                                          CsdlExpression left, CsdlExpression right, Schema schema) {
    Schema leftFieldSchema = schema.getField(SapODataConstants.LogicalExpression.LEFT_FIELD_NAME).getSchema();
    Schema rightFieldSchema = schema.getField(SapODataConstants.LogicalExpression.RIGHT_FIELD_NAME).getSchema();
    StructuredRecord leftRecord = extractExpressionRecord(fieldName, left, leftFieldSchema);
    StructuredRecord rightRecord = extractExpressionRecord(fieldName, right, rightFieldSchema);
    return StructuredRecord.builder(schema)
      .set(SapODataConstants.LogicalExpression.NAME_FIELD_NAME, logical.getType().name())
      .set(SapODataConstants.LogicalExpression.LEFT_FIELD_NAME, leftRecord)
      .set(SapODataConstants.LogicalExpression.RIGHT_FIELD_NAME, rightRecord)
      .build();
  }

  private StructuredRecord extractNotExpressionRecord(String fieldName, CsdlExpression value, Schema schema) {
    Schema leftFieldSchema = schema.getField(SapODataConstants.NotExpression.VALUE_FIELD_NAME).getSchema();
    return StructuredRecord.builder(schema)
      .set(SapODataConstants.NotExpression.NAME_FIELD_NAME, EdmExpression.EdmExpressionType.Not.name())
      .set(SapODataConstants.NotExpression.VALUE_FIELD_NAME, extractExpressionRecord(fieldName, value, leftFieldSchema))
      .build();
  }

  private StructuredRecord extractSingleValueExpressionRecord(String expressionName, Object value, Schema schema) {
    return StructuredRecord.builder(schema)
      .set(SapODataConstants.ValuedExpression.NAME_FIELD_NAME, expressionName)
      .set(SapODataConstants.ValuedExpression.VALUE_FIELD_NAME, value)
      .build();
  }

  private StructuredRecord extractApplyExpressionRecord(String fieldName, CsdlApply apply, Schema schema) {
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

      parametersRecordBuilder.set(parameterName, extractExpressionRecord(fieldName, parameter, parameterSchema));
    }

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.ApplyExpression.NAME_FIELD_NAME, EdmExpression.EdmExpressionType.Apply.name())
      .set(SapODataConstants.ApplyExpression.FUNCTION_FIELD_NAME, apply.getFunction())
      .set(SapODataConstants.ApplyExpression.PARAMETERS_FIELD_NAME, parametersRecordBuilder.build())
      .build();
  }

  private StructuredRecord extractCastExpressionRecord(String fieldName, CsdlCast cast, Schema schema) {
    Schema.Field valueField = schema.getField(SapODataConstants.CastIsOfExpression.VALUE_FIELD_NAME);
    Schema valueSchema = valueField.getSchema().isNullable() ? valueField.getSchema().getNonNullable()
      : valueField.getSchema();
    StructuredRecord valueRecord = extractExpressionRecord(fieldName, cast.getValue(), valueSchema);

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

  private StructuredRecord extractCollectionExpressionRecord(String fieldName, CsdlCollection collection,
                                                             Schema schema) {
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
      .map(i -> extractExpressionRecord(fieldName, i, componentSchema))
      .collect(Collectors.toList());

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.CollectionExpression.NAME_FIELD_NAME, expressionName)
      .set(SapODataConstants.CollectionExpression.ITEMS_FIELD_NAME, items)
      .build();
  }

  private StructuredRecord extractIfExpressionRecord(String fieldName, CsdlIf edmIf, Schema schema) {
    Schema.Field guardField = schema.getField(SapODataConstants.IfExpression.GUARD_FIELD_NAME);
    Schema guardSchema = guardField.getSchema().isNullable() ? guardField.getSchema().getNonNullable()
      : guardField.getSchema();

    StructuredRecord guardRecord = extractExpressionRecord(fieldName, edmIf.getGuard(), guardSchema);
    Schema.Field thenField = schema.getField(SapODataConstants.IfExpression.THEN_FIELD_NAME);
    Schema thenSchema = thenField.getSchema().isNullable() ? thenField.getSchema().getNonNullable()
      : thenField.getSchema();
    StructuredRecord thenRecord = extractExpressionRecord(fieldName, edmIf.getThen(), thenSchema);

    Schema.Field elseField = schema.getField(SapODataConstants.IfExpression.ELSE_FIELD_NAME);
    Schema elseSchema = elseField.getSchema().isNullable() ? elseField.getSchema().getNonNullable()
      : elseField.getSchema();
    StructuredRecord elseRecord = extractExpressionRecord(fieldName, edmIf.getElse(), elseSchema);

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.IfExpression.NAME_FIELD_NAME, EdmExpression.EdmExpressionType.If.name())
      .set(SapODataConstants.IfExpression.GUARD_FIELD_NAME, guardRecord)
      .set(SapODataConstants.IfExpression.THEN_FIELD_NAME, thenRecord)
      .set(SapODataConstants.IfExpression.ELSE_FIELD_NAME, elseRecord)
      .build();
  }

  private StructuredRecord extractIsOfExpressionRecord(String fieldName, CsdlIsOf isOf, Schema schema) {
    Schema.Field valueField = schema.getField(SapODataConstants.CastIsOfExpression.VALUE_FIELD_NAME);
    Schema valueSchema = valueField.getSchema().isNullable() ? valueField.getSchema().getNonNullable()
      : valueField.getSchema();
    StructuredRecord valueRecord = extractExpressionRecord(fieldName, isOf.getValue(), valueSchema);

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

  private StructuredRecord extractLabeledElementExpressionRecord(String fieldName, CsdlLabeledElement labeledElement,
                                                                 Schema schema) {
    Schema.Field valueField = schema.getField(SapODataConstants.LabeledElementExpression.VALUE_FIELD_NAME);
    Schema valueSchema = valueField.getSchema().isNullable() ? valueField.getSchema().getNonNullable()
      : valueField.getSchema();
    StructuredRecord valueRecord = extractExpressionRecord(fieldName, labeledElement.getValue(), valueSchema);
    String expressionName = EdmExpression.EdmExpressionType.LabeledElement.name();

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.LabeledElementExpression.NAME_FIELD_NAME, expressionName)
      .set(SapODataConstants.LabeledElementExpression.ELEMENT_NAME_FIELD_NAME, labeledElement.getName())
      .set(SapODataConstants.LabeledElementExpression.VALUE_FIELD_NAME, valueRecord)
      .build();
  }

  private StructuredRecord extractUrlRefExpressionRecord(String fieldName, CsdlUrlRef urlRef, Schema schema) {
    Schema.Field valueField = schema.getField(SapODataConstants.UrlRefExpression.VALUE_FIELD_NAME);
    Schema valueSchema = valueField.getSchema().isNullable() ? valueField.getSchema().getNonNullable()
      : valueField.getSchema();
    StructuredRecord valueRecord = extractExpressionRecord(fieldName, urlRef.getValue(), valueSchema);

    return StructuredRecord.builder(schema)
      .set(SapODataConstants.UrlRefExpression.NAME_FIELD_NAME, EdmExpression.EdmExpressionType.UrlRef.name())
      .set(SapODataConstants.UrlRefExpression.VALUE_FIELD_NAME, valueRecord)
      .build();
  }

  private StructuredRecord extractRecordExpressionRecord(CsdlRecord record, Schema schema) {
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
      StructuredRecord annotationsRecord = extractNestedAnnotationsRecord(nestedAnnotationsByName, annotationsSchema);
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
        StructuredRecord propertyRecord = extractExpressionRecord(propertyName, pv.getValue(), propertySchema);
        builder.set(propertyName, propertyRecord);
      });

    return builder.build();
  }
}
