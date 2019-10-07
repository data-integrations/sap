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

import com.google.common.base.Preconditions;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.sap.odata.ComplexPropertyMetadata;
import io.cdap.plugin.sap.odata.EntityType;
import io.cdap.plugin.sap.odata.GenericODataClient;
import io.cdap.plugin.sap.odata.ODataAnnotation;
import io.cdap.plugin.sap.odata.ODataEntity;
import io.cdap.plugin.sap.odata.ODataUtil;
import io.cdap.plugin.sap.odata.PropertyMetadata;
import io.cdap.plugin.sap.odata.exception.ODataException;
import io.cdap.plugin.sap.odata.odata2.OData2Annotation;
import io.cdap.plugin.sap.odata.odata4.OData4Annotation;
import io.cdap.plugin.sap.transformer.ODataEntryToRecordTransformer;
import io.cdap.plugin.sap.transformer.ODataEntryToRecordWithMetadataTransformer;
import org.apache.hadoop.io.NullWritable;
import org.apache.olingo.commons.api.edm.annotation.EdmExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlApply;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlIf;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlIsOf;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlLabeledElement;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlLogicalOrComparisonExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlPropertyValue;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlRecord;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlUrlRef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Plugin returns records from SAP OData service specified by URL.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SapODataConstants.PLUGIN_NAME)
@Description("Read data from SAP OData service.")
public class SapODataSource extends BatchSource<NullWritable, ODataEntity, StructuredRecord> {

  private final SapODataConfig config;
  private ODataEntryToRecordTransformer transformer;

  public SapODataSource(SapODataConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    config.validate(collector);
    try {
      // API call validation
      new GenericODataClient(config.getUrl(), config.getUser(), config.getPassword())
        .getEntitySetType(config.getResourcePath());
    } catch (ODataException e) {
      collector.addFailure("Unable to connect to OData Service: " + e.getMessage(), null)
        .withStacktrace(e.getStackTrace());
      collector.getOrThrowException();
    }

    Schema schema = getSchema();
    Schema configuredSchema = config.getParsedSchema();
    if (configuredSchema == null) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
      return;
    }

    SapODataConfig.validateFieldsMatch(schema, configuredSchema, collector);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(configuredSchema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    try {
      // API call validation
      new GenericODataClient(config.getUrl(), config.getUser(), config.getPassword())
        .getEntitySetType(config.getResourcePath());
    } catch (ODataException e) {
      collector.addFailure("Unable to connect to OData Service: " + e.getMessage(), null)
        .withStacktrace(e.getStackTrace());
      collector.getOrThrowException();
    }

    Schema schema = context.getOutputSchema();
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
    lineageRecorder.createExternalDataset(schema);
    lineageRecorder.recordRead("Read", String.format("Read resource '%s' from OData service '%s'",
                                                     config.getResourcePath(), config.getUrl()),
                               Preconditions.checkNotNull(schema.getFields()).stream()
                                 .map(Schema.Field::getName)
                                 .collect(Collectors.toList()));

    context.setInput(Input.of(config.getReferenceName(), new ODataEntryInputFormatProvider(config)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    Schema schema = context.getOutputSchema();
    this.transformer = config.isIncludeMetadataAnnotations()
      ? new ODataEntryToRecordWithMetadataTransformer(schema, getMetadataAnnotations())
      : new ODataEntryToRecordTransformer(schema);
  }

  @Override
  public void transform(KeyValue<NullWritable, ODataEntity> input, Emitter<StructuredRecord> emitter) {
    ODataEntity entity = input.getValue();
    emitter.emit(transformer.transform(entity));
  }

  public Schema getSchema() {
    GenericODataClient oDataClient = new GenericODataClient(config.getUrl(), config.getUser(), config.getPassword());
    try {
      EntityType entityType = oDataClient.getEntitySetType(config.getResourcePath());
      List<Schema.Field> fields = entityType.getProperties().stream()
        .filter(p -> config.getSelectProperties().isEmpty() || config.getSelectProperties().contains(p.getName()))
        .map(propertyMetadata -> getSchemaField(propertyMetadata, config.isIncludeMetadataAnnotations()))
        .collect(Collectors.toList());
      return Schema.recordOf("output", fields);
    } catch (ODataException e) {
      throw new InvalidStageException("Unable to get details about the entity type: " + e.getMessage(), e);
    }
  }

  private Map<String, List<ODataAnnotation>> getMetadataAnnotations() {
    GenericODataClient oDataClient = new GenericODataClient(config.getUrl(), config.getUser(), config.getPassword());
    try {
      EntityType entityType = oDataClient.getEntitySetType(config.getResourcePath());
      return entityType.getProperties().stream()
        .filter(p -> config.getSelectProperties().isEmpty() || config.getSelectProperties().contains(p.getName()))
        .collect(HashMap::new, (m, v) -> m.put(v.getName(), v.getAnnotations()), HashMap::putAll);
    } catch (ODataException e) {
      throw new InvalidStageException("Unable to get metadata annotations for the entity type: " + e.getMessage(), e);
    }
  }

  private Schema.Field getSchemaField(PropertyMetadata propertyMetadata, boolean includeAnnotations) {
    Schema nonNullableSchema = propertyToSchema(propertyMetadata);
    Schema schema = propertyMetadata.isNullable() ? Schema.nullableOf(nonNullableSchema) : nonNullableSchema;
    List<ODataAnnotation> annotations = propertyMetadata.getAnnotations();

    return includeAnnotations && annotations != null && !annotations.isEmpty()
      ? getFieldWithAnnotations(propertyMetadata, schema) : Schema.Field.of(propertyMetadata.getName(), schema);
  }

  private Schema.Field getFieldWithAnnotations(PropertyMetadata propertyMetadata, Schema valueSchema) {
    String propertyName = propertyMetadata.getName();
    List<ODataAnnotation> annotations = propertyMetadata.getAnnotations();
    List<Schema.Field> fields = annotations.stream()
      .map(annotation -> Schema.Field.of(annotation.getName(), annotationToFieldSchema(propertyName, annotation)))
      .collect(Collectors.toList());
    Schema.Field metadataField = Schema.Field.of(SapODataConstants.METADATA_ANNOTATIONS_FIELD_NAME,
                                                 Schema.recordOf(propertyName + "-metadata-record", fields));

    Schema valueWithMetadataSchema = Schema.recordOf(propertyName + "-value-with-metadata-record",
                                                     Schema.Field.of(SapODataConstants.VALUE_FIELD_NAME, valueSchema),
                                                     metadataField);
    return Schema.Field.of(propertyName, valueWithMetadataSchema);
  }

  private Schema annotationToFieldSchema(String fieldName, ODataAnnotation oDataAnnotation) {
    if (oDataAnnotation instanceof OData2Annotation) {
      // OData 2 annotations are strings
      return Schema.of(Schema.Type.STRING);
    }

    // OData 4 annotations
    OData4Annotation annotation = (OData4Annotation) oDataAnnotation;
    CsdlExpression expression = annotation.getExpression();
    Schema expressionSchema = expressionToFieldSchema(fieldName, expression);
    Schema annotationsSchema = nestedAnnotationsSchema(annotation.getName(), annotation.getAnnotations());
    String recordName = fieldName + "-" + annotation.getName();
    return SapODataConstants.Annotation.schema(recordName, expressionSchema, annotationsSchema);
  }

  private @Nullable Schema nestedAnnotationsSchema(String annotationName, Map<String, OData4Annotation> annotations) {
    if (annotations.isEmpty()) {
      return null;
    }
    List<Schema.Field> fields = annotations.values().stream()
      .map(annotation -> Schema.Field.of(annotation.getName(), annotationToFieldSchema(annotationName, annotation)))
      .collect(Collectors.toList());

    return Schema.recordOf(annotationName + "-nested-annotations", fields);
  }

  private @Nullable Schema expressionToFieldSchema(String fieldName, CsdlExpression expression) {
    if (expression == null) {
      return null;
    }
    EdmExpression.EdmExpressionType type = ODataUtil.typeOf(expression);
    String recordName = String.format("%s-%s", fieldName, type);
    switch (type) {
      case Null:
        // Null annotations are not included. Olingo V4 client does not parse expression annotations correctly:
        // https://issues.apache.org/jira/browse/OLINGO-1403
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
      case Path:
      case AnnotationPath:
      case LabeledElementReference:
      case NavigationPropertyPath:
      case PropertyPath:
        return SapODataConstants.ValuedExpression.SCHEMA;
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
        Schema andLeft = expressionToFieldSchema(fieldName, logicalOrComparison.getLeft());
        Schema andRight = expressionToFieldSchema(fieldName, logicalOrComparison.getRight());
        return SapODataConstants.LogicalExpression.schema(recordName, andLeft, andRight);
      case Not:
        // Negation expressions are represented as an element edm:Not that MUST contain a single annotation expression.
        // See 'Comparison and Logical Operators' section of
        // https://docs.oasis-open.org/odata/odata-csdl-xml/v4.01/csprd05/odata-csdl-xml-v4.01-csprd05.html
        // However, Olingo represents Not via common CsdlLogicalOrComparisonExpression, which is  common for all
        // logical or comparison expressions. Thus, value expression can be accessed via either 'getLeft' or 'getRight'.
        // See CsdlLogicalOrComparisonExpression#setRight implementation for details
        CsdlLogicalOrComparisonExpression not = expression.asDynamic().asLogicalOrComparison();
        Schema value = expressionToFieldSchema(fieldName, not.getLeft());
        return SapODataConstants.NotExpression.schema(recordName, value);
      case Apply:
        // Apply annotations are not included. Olingo V4 client does not parse expression annotations correctly:
        // https://issues.apache.org/jira/browse/OLINGO-1403
        List<Schema.Field> parameterFields = new ArrayList<>();
        CsdlApply apply = expression.asDynamic().asApply();
        List<CsdlExpression> parameters = apply.getParameters();
        for (int i = 0; i < parameters.size(); i++) {
          CsdlExpression parameter = parameters.get(i);
          String parameterName = String.format("%s_%d", ODataUtil.typeOf(parameter), i);
          Schema.Field field = Schema.Field.of(parameterName, expressionToFieldSchema(parameterName, parameter));
          parameterFields.add(field);
        }
        Schema parametersSchema = Schema.recordOf(fieldName + "-parameters", parameterFields);
        return SapODataConstants.ApplyExpression.schema(fieldName, parametersSchema);
      case Cast:
        // Cast annotations are not included. Olingo V4 client does not parse expression annotations correctly:
        // https://issues.apache.org/jira/browse/OLINGO-1403
        Schema expressionSchema = expressionToFieldSchema(fieldName, expression.asDynamic().asCast().getValue());
        return SapODataConstants.CastIsOfExpression.schema(fieldName, expressionSchema);
      case Collection:
        List<CsdlExpression> items = expression.asDynamic().asCollection().getItems();
        // The values of the child expressions MUST all be type compatible.
        Schema itemSchema = items == null || items.isEmpty() ? null : expressionToFieldSchema(fieldName, items.get(0));
        return SapODataConstants.CollectionExpression.schema(fieldName, itemSchema);
      case If:
        CsdlIf csdlIf = expression.asDynamic().asIf();
        Schema guard = expressionToFieldSchema(fieldName, csdlIf.getGuard());
        Schema then = expressionToFieldSchema(fieldName, csdlIf.getThen());
        Schema elseSchema = expressionToFieldSchema(fieldName, csdlIf.getElse());
        return SapODataConstants.IfExpression.schema(fieldName, guard, then, elseSchema);
      case IsOf:
        // IsOf annotations are not included. Olingo V4 client does not parse expression annotations correctly:
        // https://issues.apache.org/jira/browse/OLINGO-1403
        CsdlIsOf isOf = expression.asDynamic().asIsOf();
        Schema valueSchema = expressionToFieldSchema(fieldName, isOf.getValue());
        return SapODataConstants.CastIsOfExpression.schema(fieldName, valueSchema);
      case LabeledElement:
        // LabeledElement annotations are not included.
        // Olingo V4 client does not parse expression annotations correctly:
        // https://issues.apache.org/jira/browse/OLINGO-1403
        CsdlLabeledElement labeledElement = expression.asDynamic().asLabeledElement();
        Schema labeledElementSchema = expressionToFieldSchema(fieldName, labeledElement.getValue());
        return SapODataConstants.LabeledElementExpression.schema(fieldName, labeledElementSchema);
      case Record:
        CsdlRecord record = expression.asDynamic().asRecord();
        Schema propertyValuesSchema = propertyValuesSchema(fieldName, record.getPropertyValues());
        if (record.getAnnotations() == null || record.getAnnotations().isEmpty()) {
          return SapODataConstants.RecordExpression.schema(fieldName, propertyValuesSchema, null);
        }
        Map<String, OData4Annotation> nestedRecordAnnotations = record.getAnnotations().stream()
          .map(OData4Annotation::new)
          .collect(Collectors.toMap(OData4Annotation::getName, Function.identity()));
        Schema recordAnnotationsSchema = nestedAnnotationsSchema(recordName, nestedRecordAnnotations);
        return SapODataConstants.RecordExpression.schema(fieldName, propertyValuesSchema, recordAnnotationsSchema);
      case UrlRef:
        CsdlUrlRef urlRef = expression.asDynamic().asUrlRef();
        Schema urlRefValueSchema = expressionToFieldSchema(fieldName, urlRef.getValue());
        return SapODataConstants.UrlRefExpression.schema(fieldName, urlRefValueSchema);
      default:
        // this should never happen
        throw new InvalidStageException(
          String.format("Annotation expression for field '%s' is of unsupported type '%s'.", fieldName, type));
    }
  }

  private Schema propertyValuesSchema(String fieldName, List<CsdlPropertyValue> propertyValues) {
    if (propertyValues == null || propertyValues.isEmpty()) {
      return null;
    }
    List<Schema.Field> fields = propertyValues.stream()
      .map(p -> Schema.Field.of(p.getProperty(), expressionToFieldSchema(fieldName, p.getValue())))
      .collect(Collectors.toList());

    return Schema.recordOf(fieldName + "-property-values", fields);
  }

  private Schema propertyToSchema(PropertyMetadata propertyMetadata) {
    Schema schema = propertyTypeToSchema(propertyMetadata);
    return propertyMetadata.isCollection() ? Schema.arrayOf(schema) : schema;
  }

  private Schema propertyTypeToSchema(PropertyMetadata propertyMetadata) {
    if (propertyMetadata.isComplex()) {
      return convertComplexProperty(propertyMetadata.getName(), (ComplexPropertyMetadata) propertyMetadata);
    }
    if (propertyMetadata.isEnum()) {
      return Schema.of(Schema.Type.STRING);
    }
    switch (propertyMetadata.getType()) {
      case "Binary":
      case "Edm.Binary":
        return Schema.of(Schema.Type.BYTES);
      case "Boolean":
      case "Edm.Boolean":
        return Schema.of(Schema.Type.BOOLEAN);
      case "Byte":
      case "Edm.Byte":
        return Schema.of(Schema.Type.INT);
      case "SByte":
      case "Edm.SByte":
        return Schema.of(Schema.Type.INT);
      case "DateTime":
      case "Edm.DateTime":
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      case "DateTimeOffset":
      case "Edm.DateTimeOffset":
        // Mapped to 'string' to avoid timezone information loss
        return Schema.of(Schema.Type.STRING);
      case "Time":
      case "Edm.Time":
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      case "Decimal":
      case "Edm.Decimal":
        return Schema.decimalOf(propertyMetadata.getPrecision(), propertyMetadata.getScale());
      case "Double":
      case "Edm.Double":
        return Schema.of(Schema.Type.DOUBLE);
      case "Single":
      case "Edm.Single":
        return Schema.of(Schema.Type.FLOAT);
      case "Guid":
      case "Edm.Guid":
        return Schema.of(Schema.Type.STRING);
      case "Int16":
      case "Edm.Int16":
        return Schema.of(Schema.Type.INT);
      case "Int32":
      case "Edm.Int32":
        return Schema.of(Schema.Type.INT);
      case "Int64":
      case "Edm.Int64":
        return Schema.of(Schema.Type.LONG);
      case "String":
      case "Edm.String":
        return Schema.of(Schema.Type.STRING);
      case "GeographyPoint":
      case "Edm.GeographyPoint":
      case "GeometryPoint":
      case "Edm.GeometryPoint":
        return SapODataConstants.Point.SCHEMA;
      case "GeographyLineString":
      case "Edm.GeographyLineString":
      case "GeometryLineString":
      case "Edm.GeometryLineString":
        return SapODataConstants.LineString.SCHEMA;
      case "GeographyPolygon":
      case "Edm.GeographyPolygon":
      case "GeometryPolygon":
      case "Edm.GeometryPolygon":
        return SapODataConstants.Polygon.SCHEMA;
      case "GeographyMultiPoint":
      case "Edm.GeographyMultiPoint":
      case "GeometryMultiPoint":
      case "Edm.GeometryMultiPoint":
        return SapODataConstants.MultiPoint.SCHEMA;
      case "GeographyMultiLineString":
      case "Edm.GeographyMultiLineString":
      case "GeometryMultiLineString":
      case "Edm.GeometryMultiLineString":
        return SapODataConstants.MultiLineString.SCHEMA;
      case "GeographyMultiPolygon":
      case "Edm.GeographyMultiPolygon":
      case "GeometryMultiPolygon":
      case "Edm.GeometryMultiPolygon":
        return SapODataConstants.MultiPolygon.SCHEMA;
      case "GeographyCollection":
      case "Edm.GeographyCollection":
      case "GeometryCollection":
      case "Edm.GeometryCollection":
        return SapODataConstants.GeospatialCollection.SCHEMA;
      case "Date":
      case "Edm.Date":
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      case "Duration":
      case "Edm.Duration":
        return Schema.of(Schema.Type.STRING);
      case "Stream":
      case "Edm.Stream":
        return SapODataConstants.Stream.SCHEMA;
      case "TimeOfDay":
      case "Edm.TimeOfDay":
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      default:
        // this should never happen
        throw new InvalidStageException(String.format("Field '%s' is of unsupported type '%s'.",
                                                      propertyMetadata.getName(), propertyMetadata.getType()));
    }
  }

  private Schema convertComplexProperty(String name, ComplexPropertyMetadata propertyMetadata) {
    List<Schema.Field> fields = propertyMetadata.getProperties().stream()
      .map(p -> Schema.Field.of(p.getName(), propertyToSchema(p)))
      .collect(Collectors.toList());
    return Schema.recordOf(name + "-complex-type-values", fields);
  }
}
