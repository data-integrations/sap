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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.olingo.commons.api.edm.annotation.EdmExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Provides handy methods to construct an annotation {@link StructuredRecord} instance for testing.
 */
public class AnnotationRecordBuilder {

  private final String fieldName;
  private String fullyQualifiedTermName;
  private String qualifier;
  private StructuredRecord expression;
  private Map<String, StructuredRecord> annotations = new HashMap<>();

  private AnnotationRecordBuilder(String fieldName) {
    this.fieldName = fieldName;
  }

  public static AnnotationRecordBuilder builder(String fieldName) {
    return new AnnotationRecordBuilder(fieldName);
  }

  public AnnotationRecordBuilder setTerm(String fullyQualifiedTermName) {
    this.fullyQualifiedTermName = fullyQualifiedTermName;
    return this;
  }

  public AnnotationRecordBuilder setQualifier(String qualifier) {
    this.qualifier = qualifier;
    return this;
  }

  public AnnotationRecordBuilder withConstantExpression(EdmExpression.EdmExpressionType type, String value) {
    this.expression = singleValueExpression(type.name(), value);
    return this;
  }

  public ApplyExpressionBuilder withApplyExpression(String function) {
    return new ApplyExpressionBuilder(function);
  }

  public LogicalExpressionBuilder withLogicalExpression(EdmExpression.EdmExpressionType type) {
    return new LogicalExpressionBuilder(type);
  }

  public NotExpressionBuilder withNotExpression() {
    return new NotExpressionBuilder();
  }

  public UrlRefExpressionBuilder withUrlRefExpression() {
    return new UrlRefExpressionBuilder();
  }

  public CastExpressionBuilder withCastExpression(String type) {
    return new CastExpressionBuilder(type);
  }

  public IsOfExpressionBuilder withIsOfExpression(String type) {
    return new IsOfExpressionBuilder(type);
  }

  public CollectionExpressionBuilder withCollectionExpression() {
    return new CollectionExpressionBuilder();
  }

  public IfExpressionBuilder withIfExpression() {
    return new IfExpressionBuilder();
  }

  public LabeledElementExpressionBuilder withLabeledElementExpression(String elementName) {
    return new LabeledElementExpressionBuilder(elementName);
  }

  public RecordExpressionBuilder withRecordExpression() {
    return new RecordExpressionBuilder();
  }

  public AnnotationRecordBuilder withAnnotation(String name, StructuredRecord annotation) {
    annotations.put(name, annotation);
    return this;
  }

  public StructuredRecord build() {
    if (annotations.isEmpty()) {
      Schema annotationSchema = SapODataConstants.Annotation.schema(fieldName, expression.getSchema(), null);
      return StructuredRecord.builder(annotationSchema)
        .set(SapODataConstants.Annotation.TERM_FIELD_NAME, fullyQualifiedTermName)
        .set(SapODataConstants.Annotation.EXPRESSION_FIELD_NAME, expression)
        .set(SapODataConstants.Annotation.QUALIFIER_FIELD_NAME, qualifier)
        .build();
    }

    List<Schema.Field> fields = annotations.entrySet().stream()
      .map(entry -> Schema.Field.of(entry.getKey(), entry.getValue().getSchema()))
      .collect(Collectors.toList());
    Schema annotationsSchema = Schema.recordOf(fieldName + "-nested-annotations", fields);
    StructuredRecord.Builder nestedAnnotationsBuilder = StructuredRecord.builder(annotationsSchema);
    annotations.entrySet().forEach(entry -> nestedAnnotationsBuilder.set(entry.getKey(), entry.getValue()));

    Schema annotationSchema = SapODataConstants.Annotation.schema(fieldName, expression.getSchema(), annotationsSchema);
    return StructuredRecord.builder(annotationSchema)
      .set(SapODataConstants.Annotation.TERM_FIELD_NAME, fullyQualifiedTermName)
      .set(SapODataConstants.Annotation.EXPRESSION_FIELD_NAME, expression)
      .set(SapODataConstants.Annotation.QUALIFIER_FIELD_NAME, qualifier)
      .set(SapODataConstants.Annotation.ANNOTATIONS_FIELD_NAME, nestedAnnotationsBuilder.build())
      .build();
  }

  protected StructuredRecord singleValueExpression(String expressionName, Object value) {
    return singleValueExpression(fieldName, expressionName, value);
  }

  protected StructuredRecord singleValueExpression(String recordName, String expressionName, Object value) {
    return StructuredRecord.builder(SapODataConstants.ValuedExpression.SCHEMA)
      .set(SapODataConstants.ValuedExpression.NAME_FIELD_NAME, expressionName)
      .set(SapODataConstants.ValuedExpression.VALUE_FIELD_NAME, value)
      .build();
  }

  public class ApplyExpressionBuilder {

    private final String function;
    private List<StructuredRecord> expressions;

    ApplyExpressionBuilder(String function) {
      this.function = function;
      this.expressions = new ArrayList<>();
    }

    public ApplyExpressionBuilder withConstantExpression(EdmExpression.EdmExpressionType type, String value) {
      String recordName = String.format("%s_%d", type.name(), expressions.size());
      StructuredRecord expression = singleValueExpression(recordName, type.name(), value);
      expressions.add(expression);
      return this;
    }

    public AnnotationRecordBuilder add() {
      StructuredRecord parameters = buildApplyParametersRecord();
      Schema expressionSchema = SapODataConstants.ApplyExpression.schema(fieldName, parameters.getSchema());
      expression = StructuredRecord.builder(expressionSchema)
        .set(SapODataConstants.ApplyExpression.NAME_FIELD_NAME, "Apply")
        .set(SapODataConstants.ApplyExpression.PARAMETERS_FIELD_NAME, parameters)
        .set(SapODataConstants.ApplyExpression.FUNCTION_FIELD_NAME, function)
        .build();
      return AnnotationRecordBuilder.this;
    }

    private StructuredRecord buildApplyParametersRecord() {
      List<Schema.Field> fields = new ArrayList<>();
      for (int i = 0; i < expressions.size(); i++) {
        StructuredRecord e = expressions.get(i);
        String expressionName = e.get(SapODataConstants.ApplyExpression.NAME_FIELD_NAME);
        String parameterName = String.format("%s_%d", expressionName, i);
        Schema.Field field = Schema.Field.of(parameterName, e.getSchema());
        fields.add(field);
      }

      Schema schema = Schema.recordOf(fieldName + "-parameters", fields);
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      for (int i = 0; i < expressions.size(); i++) {
        StructuredRecord e = expressions.get(i);
        String expressionName = e.get(SapODataConstants.ApplyExpression.NAME_FIELD_NAME);
        String parameterName = String.format("%s_%d", expressionName, i);
        builder.set(parameterName, e);
      }

      return builder.build();
    }
  }

  public class LogicalExpressionBuilder {

    private final EdmExpression.EdmExpressionType type;
    private StructuredRecord left;
    private StructuredRecord right;

    LogicalExpressionBuilder(EdmExpression.EdmExpressionType type) {
      this.type = type;
    }

    public LogicalExpressionBuilder left(EdmExpression.EdmExpressionType type, String value) {
      this.left = singleValueExpression(type.name(), value);
      return this;
    }

    public LogicalExpressionBuilder right(EdmExpression.EdmExpressionType type, String value) {
      this.right = singleValueExpression(type.name(), value);
      return this;
    }

    public AnnotationRecordBuilder add() {
      Schema schema = SapODataConstants.LogicalExpression.schema(fieldName, left.getSchema(), right.getSchema());
      expression = StructuredRecord.builder(schema)
        .set(SapODataConstants.LogicalExpression.NAME_FIELD_NAME, type.name())
        .set(SapODataConstants.LogicalExpression.LEFT_FIELD_NAME, left)
        .set(SapODataConstants.LogicalExpression.RIGHT_FIELD_NAME, right)
        .build();
      return AnnotationRecordBuilder.this;
    }
  }

  public class IfExpressionBuilder {

    private StructuredRecord guard;
    private StructuredRecord then;
    private StructuredRecord elseRecord;

    public IfExpressionBuilder withGuard(EdmExpression.EdmExpressionType type, String value) {
      this.guard = singleValueExpression(type.name(), value);
      return this;
    }

    public IfExpressionBuilder withThen(EdmExpression.EdmExpressionType type, String value) {
      this.then = singleValueExpression(type.name(), value);
      return this;
    }

    public IfExpressionBuilder withElse(EdmExpression.EdmExpressionType type, String value) {
      this.elseRecord = singleValueExpression(type.name(), value);
      return this;
    }

    public AnnotationRecordBuilder add() {
      Schema schema = SapODataConstants.IfExpression.schema(fieldName, guard.getSchema(), then.getSchema(),
                                                            elseRecord.getSchema());
      expression = StructuredRecord.builder(schema)
        .set(SapODataConstants.IfExpression.NAME_FIELD_NAME, "If")
        .set(SapODataConstants.IfExpression.GUARD_FIELD_NAME, guard)
        .set(SapODataConstants.IfExpression.THEN_FIELD_NAME, then)
        .set(SapODataConstants.IfExpression.ELSE_FIELD_NAME, elseRecord)
        .build();
      return AnnotationRecordBuilder.this;
    }
  }

  public class NotExpressionBuilder {

    protected EdmExpression.EdmExpressionType type;
    private StructuredRecord value;

    NotExpressionBuilder() {
      type = EdmExpression.EdmExpressionType.Not;
    }

    public NotExpressionBuilder value(EdmExpression.EdmExpressionType type, String value) {
      this.value = singleValueExpression(type.name(), value);
      return this;
    }

    public AnnotationRecordBuilder add() {
      Schema expressionSchema = SapODataConstants.NotExpression.schema(fieldName, value.getSchema());
      expression = StructuredRecord.builder(expressionSchema)
        .set(SapODataConstants.NotExpression.NAME_FIELD_NAME, type.name())
        .set(SapODataConstants.NotExpression.VALUE_FIELD_NAME, value)
        .build();
      return AnnotationRecordBuilder.this;
    }
  }

  public class UrlRefExpressionBuilder extends NotExpressionBuilder {
    UrlRefExpressionBuilder() {
      type = EdmExpression.EdmExpressionType.UrlRef;
    }
  }

  public class LabeledElementExpressionBuilder {

    private final String elementName;
    private StructuredRecord value;

    LabeledElementExpressionBuilder(String elementName) {
      this.elementName = elementName;
    }

    public LabeledElementExpressionBuilder value(EdmExpression.EdmExpressionType type, String value) {
      this.value = singleValueExpression(type.name(), value);
      return this;
    }

    public AnnotationRecordBuilder add() {
      Schema expressionSchema = SapODataConstants.LabeledElementExpression.schema(fieldName, value.getSchema());
      expression = StructuredRecord.builder(expressionSchema)
        .set(SapODataConstants.LabeledElementExpression.NAME_FIELD_NAME, "LabeledElement")
        .set(SapODataConstants.LabeledElementExpression.ELEMENT_NAME_FIELD_NAME, elementName)
        .set(SapODataConstants.LabeledElementExpression.VALUE_FIELD_NAME, value)
        .build();
      return AnnotationRecordBuilder.this;
    }
  }

  public class CollectionExpressionBuilder {

    private List<StructuredRecord> items = new ArrayList<>();

    public CollectionExpressionBuilder withItem(EdmExpression.EdmExpressionType type, String value) {
      StructuredRecord item = singleValueExpression(type.name(), value);
      items.add(item);
      return this;
    }

    public AnnotationRecordBuilder add() {
      Schema schema = SapODataConstants.CollectionExpression.schema(fieldName, items.get(0).getSchema());
      expression = StructuredRecord.builder(schema)
        .set(SapODataConstants.CollectionExpression.NAME_FIELD_NAME, "Collection")
        .set(SapODataConstants.CollectionExpression.ITEMS_FIELD_NAME, items)
        .build();
      return AnnotationRecordBuilder.this;
    }
  }

  public class RecordExpressionBuilder {

    private String type;
    private Map<String, StructuredRecord> properties = new HashMap<>();
    private Map<String, StructuredRecord> annotations = new HashMap<>();

    public RecordExpressionBuilder withProperty(String name, EdmExpression.EdmExpressionType type, String value) {
      properties.put(name, singleValueExpression(type.name(), value));
      return this;
    }

    public RecordExpressionBuilder withType(String type) {
      this.type = type;
      return this;
    }

    public RecordExpressionBuilder withAnnotation(String name, StructuredRecord annotation) {
      this.annotations.put(name, annotation);
      return this;
    }

    public AnnotationRecordBuilder add() {
      StructuredRecord properties = buildPropertyValuesRecord(this.properties);

      if (annotations.isEmpty()) {
        Schema expressionSchema = SapODataConstants.RecordExpression.schema(fieldName, properties.getSchema(), null);
        expression = StructuredRecord.builder(expressionSchema)
          .set(SapODataConstants.RecordExpression.NAME_FIELD_NAME, "Record")
          .set(SapODataConstants.RecordExpression.TYPE_FIELD_NAME, type)
          .set(SapODataConstants.RecordExpression.PROPERTY_VALUES_FIELD_NAME, properties)
          .build();
        return AnnotationRecordBuilder.this;
      }

      List<Schema.Field> fields = annotations.entrySet().stream()
        .map(entry -> Schema.Field.of(entry.getKey(), entry.getValue().getSchema()))
        .collect(Collectors.toList());
      Schema annotationsSchema = Schema.recordOf(fieldName + "-nested-annotations", fields);
      StructuredRecord.Builder nestedAnnotationsBuilder = StructuredRecord.builder(annotationsSchema);
      annotations.entrySet().forEach(entry -> nestedAnnotationsBuilder.set(entry.getKey(), entry.getValue()));

      Schema expressionSchema = SapODataConstants.RecordExpression.schema(fieldName, properties.getSchema(),
                                                                          annotationsSchema);
      expression = StructuredRecord.builder(expressionSchema)
        .set(SapODataConstants.RecordExpression.NAME_FIELD_NAME, "Record")
        .set(SapODataConstants.RecordExpression.TYPE_FIELD_NAME, type)
        .set(SapODataConstants.RecordExpression.PROPERTY_VALUES_FIELD_NAME, properties)
        .set(SapODataConstants.RecordExpression.ANNOTATIONS_FIELD_NAME, nestedAnnotationsBuilder.build())
        .build();

      return AnnotationRecordBuilder.this;
    }

    private StructuredRecord buildPropertyValuesRecord(Map<String, StructuredRecord> propertyValues) {
      List<Schema.Field> fields = propertyValues.entrySet().stream()
        .map(e -> Schema.Field.of(e.getKey(), e.getValue().getSchema()))
        .collect(Collectors.toList());
      Schema schema = Schema.recordOf(fieldName + "-property-values", fields);
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);

      propertyValues.entrySet().forEach(e -> builder.set(e.getKey(), e.getValue()));

      return builder.build();
    }
  }

  public class CastExpressionBuilder {

    protected EdmExpression.EdmExpressionType expressionType;
    private final String type;
    private StructuredRecord value;
    private String maxLength;
    private String precision;
    private String scale;
    private String srid;

    CastExpressionBuilder(String type) {
      this.type = type;
      this.expressionType = EdmExpression.EdmExpressionType.Cast;
    }

    public CastExpressionBuilder value(EdmExpression.EdmExpressionType type, String value) {
      this.value = singleValueExpression(type.name(), value);
      return this;
    }

    public CastExpressionBuilder setMaxLength(String maxLength) {
      this.maxLength = maxLength;
      return this;
    }

    public CastExpressionBuilder setPrecision(String precision) {
      this.precision = precision;
      return this;
    }

    public CastExpressionBuilder setScale(String scale) {
      this.scale = scale;
      return this;
    }

    public CastExpressionBuilder setSrid(String srid) {
      this.srid = srid;
      return this;
    }

    public AnnotationRecordBuilder add() {
      Schema expressionSchema = SapODataConstants.CastIsOfExpression.schema(fieldName, value.getSchema());
      expression = StructuredRecord.builder(expressionSchema)
        .set(SapODataConstants.CastIsOfExpression.NAME_FIELD_NAME, expressionType.name())
        .set(SapODataConstants.CastIsOfExpression.TYPE_FIELD_NAME, type)
        .set(SapODataConstants.CastIsOfExpression.VALUE_FIELD_NAME, value)
        .set(SapODataConstants.CastIsOfExpression.MAX_LENGTH_FIELD_NAME, maxLength)
        .set(SapODataConstants.CastIsOfExpression.PRECISION_FIELD_NAME, precision)
        .set(SapODataConstants.CastIsOfExpression.SCALE_FIELD_NAME, scale)
        .set(SapODataConstants.CastIsOfExpression.SRID_FIELD_NAME, srid)
        .build();

      return AnnotationRecordBuilder.this;
    }
  }

  public class IsOfExpressionBuilder extends CastExpressionBuilder {
    IsOfExpressionBuilder(String type) {
      super(type);
      expressionType = EdmExpression.EdmExpressionType.IsOf;
    }
  }
}
