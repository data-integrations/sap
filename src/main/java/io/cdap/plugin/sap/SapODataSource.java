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
import java.util.stream.Collectors;

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
    Schema nonNullableSchema = convertPropertyType(propertyMetadata);
    Schema schema = propertyMetadata.isNullable() ? Schema.nullableOf(nonNullableSchema) : nonNullableSchema;

    return includeAnnotations && propertyMetadata.getAnnotations() != null ?
      getFieldWithAnnotations(propertyMetadata, schema) : Schema.Field.of(propertyMetadata.getName(), schema);
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
    return SapODataSchemas.annotationSchema(fieldName, expressionSchema);
  }

  // TODO nested annotations
  private Schema expressionToFieldSchema(String fieldName, CsdlExpression expression) {
    EdmExpression.EdmExpressionType type = ODataUtil.typeOf(expression);
    String recordName = String.format("%s-%s", fieldName, type);
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
      case Path:
      case AnnotationPath:
      case LabeledElementReference:
      case Null:
      case NavigationPropertyPath:
      case PropertyPath:
        return SapODataSchemas.singleValueExpressionSchema(fieldName);
      case And:
      case Or:
      case Eq:
      case Ne:
      case Gt:
      case Ge:
      case Lt:
      case Le:
        CsdlLogicalOrComparisonExpression logicalOrComparison = expression.asDynamic().asLogicalOrComparison();
        Schema andLeft = expressionToFieldSchema(fieldName, logicalOrComparison.getLeft());
        Schema andRight = expressionToFieldSchema(fieldName, logicalOrComparison.getRight());
        return SapODataSchemas.logicalExpressionSchema(recordName, andLeft, andRight);
      case Not:
        // Negation expressions are represented as an element edm:Not that MUST contain a single annotation expression.
        // See 'Comparison and Logical Operators' section of
        // https://docs.oasis-open.org/odata/odata-csdl-xml/v4.01/csprd05/odata-csdl-xml-v4.01-csprd05.html
        // However, Olingo represents Not via common CsdlLogicalOrComparisonExpression, which is  common for all
        // logical or comparison expressions. Thus, value expression can be accessed via either 'getLeft' or 'getRight'.
        // See CsdlLogicalOrComparisonExpression#setRight implementation for details
        CsdlLogicalOrComparisonExpression not = expression.asDynamic().asLogicalOrComparison();
        Schema value = expressionToFieldSchema(fieldName, not.getLeft());
        return SapODataSchemas.notExpressionSchema(recordName, value);
      case Apply:
        List<Schema.Field> parameterFields = new ArrayList<>();
        List<CsdlExpression> parameters = expression.asDynamic().asApply().getParameters();
        for (int i = 0; i < parameters.size(); i++) {
          CsdlExpression parameter = parameters.get(i);
          String parameterName = String.format("%d-%s", i, ODataUtil.typeOf(parameter));
          Schema.Field field = Schema.Field.of(parameterName, expressionToFieldSchema(fieldName, parameter));
          parameterFields.add(field);
        }
        Schema parametersSchema = Schema.recordOf(fieldName + "-parameters", parameterFields);
        return SapODataSchemas.applyExpressionSchema(fieldName, parametersSchema);
      case Cast:
        Schema expressionSchema = expressionToFieldSchema(fieldName, expression.asDynamic().asCast().getValue());
        return SapODataSchemas.castExpressionSchema(fieldName, expressionSchema);
      case Collection:
        List<CsdlExpression> items = expression.asDynamic().asCollection().getItems();
        // The values of the child expressions MUST all be type compatible.
        Schema itemSchema = items == null || items.isEmpty() ? null : expressionToFieldSchema(fieldName, items.get(0));
        return SapODataSchemas.collectionExpressionSchema(fieldName, itemSchema);
      case If:
        CsdlIf csdlIf = expression.asDynamic().asIf();
        Schema guard = expressionToFieldSchema(fieldName, csdlIf.getGuard());
        Schema then = expressionToFieldSchema(fieldName, csdlIf.getThen());
        Schema elseSchema = expressionToFieldSchema(fieldName, csdlIf.getElse());
        return SapODataSchemas.ifExpressionSchema(fieldName, guard, then, elseSchema);
      case IsOf:
        CsdlIsOf isOf = expression.asDynamic().asIsOf();
        return SapODataSchemas.isOfExpressionSchema(fieldName, expressionToFieldSchema(fieldName, isOf.getValue()));
      case LabeledElement:
        CsdlLabeledElement labeledElement = expression.asDynamic().asLabeledElement();
        Schema labeledElementSchema = expressionToFieldSchema(fieldName, labeledElement.getValue());
        return SapODataSchemas.labeledElementExpressionSchema(fieldName, labeledElementSchema);
      case Record:
        CsdlRecord record = expression.asDynamic().asRecord();
        Schema propertyValuesSchema = propertyValuesSchema(fieldName, record.getPropertyValues());
        return SapODataSchemas.recordExpressionSchema(fieldName, propertyValuesSchema);
      case UrlRef:
        CsdlUrlRef urlRef = expression.asDynamic().asUrlRef();
        return SapODataSchemas.urlRefExpressionSchema(fieldName, expressionToFieldSchema(fieldName, urlRef.getValue()));
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

  private Schema convertPropertyType(PropertyMetadata propertyMetadata) {
    switch (propertyMetadata.getEdmTypeName()) {
      case "Edm.Binary":
        return Schema.of(Schema.Type.BYTES);
      case "Edm.Boolean":
        return Schema.of(Schema.Type.BOOLEAN);
      case "Edm.Byte":
        return Schema.of(Schema.Type.INT);
      case "Edm.SByte":
        return Schema.of(Schema.Type.INT);
      case "Edm.DateTime":
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      case "Edm.DateTimeOffset":
        // Mapped to 'string' to avoid timezone information loss
        return Schema.of(Schema.Type.STRING);
      case "Edm.Time":
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      case "Edm.Decimal":
        return Schema.decimalOf(propertyMetadata.getPrecision(), propertyMetadata.getScale());
      case "Edm.Double":
        return Schema.of(Schema.Type.DOUBLE);
      case "Edm.Single":
        return Schema.of(Schema.Type.FLOAT);
      case "Edm.Guid":
        return Schema.of(Schema.Type.STRING);
      case "Edm.Int16":
        return Schema.of(Schema.Type.INT);
      case "Edm.Int32":
        return Schema.of(Schema.Type.INT);
      case "Edm.Int64":
        return Schema.of(Schema.Type.LONG);
      case "Edm.String":
        return Schema.of(Schema.Type.STRING);
      case "Edm.GeographyPoint":
      case "Edm.GeometryPoint":
        return SapODataConstants.Point.SCHEMA;
      case "Edm.GeographyLineString":
      case "Edm.GeometryLineString":
        return SapODataConstants.LineString.SCHEMA;
      case "Edm.GeographyPolygon":
      case "Edm.GeometryPolygon":
        return SapODataConstants.Polygon.SCHEMA;
      case "Edm.GeographyMultiPoint":
      case "Edm.GeometryMultiPoint":
        return SapODataConstants.MultiPoint.SCHEMA;
      case "Edm.GeographyMultiLineString":
      case "Edm.GeometryMultiLineString":
        return SapODataConstants.MultiLineString.SCHEMA;
      case "Edm.GeographyMultiPolygon":
      case "Edm.GeometryMultiPolygon":
        return SapODataConstants.MultiPolygon.SCHEMA;
      case "Edm.GeographyCollection":
      case "Edm.GeometryCollection":
        return SapODataConstants.GeospatialCollection.SCHEMA;
      case "Edm.Date":
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      case "Edm.Duration":
        return Schema.of(Schema.Type.STRING);
      case "Edm.Stream":
        return SapODataConstants.Stream.SCHEMA;
      case "Edm.TimeOfDay":
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      default:
        // this should never happen
        throw new InvalidStageException(String.format("Field '%s' is of unsupported type '%s'.",
                                                      propertyMetadata.getName(), propertyMetadata.getEdmTypeName()));
    }
  }
}
