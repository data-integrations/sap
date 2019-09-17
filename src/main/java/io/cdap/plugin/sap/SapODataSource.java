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
import io.cdap.plugin.sap.odata.PropertyMetadata;
import io.cdap.plugin.sap.odata.exception.ODataException;
import io.cdap.plugin.sap.odata.odata2.OData2Annotation;
import io.cdap.plugin.sap.odata.odata4.OData4Annotation;
import io.cdap.plugin.sap.transformer.ODataEntryToRecordTransformer;
import io.cdap.plugin.sap.transformer.ODataEntryToRecordWithMetadataTransformer;
import org.apache.hadoop.io.NullWritable;
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
    EdmExpression expression = annotation.getExpression();
    Schema expressionSchema = expressionToFieldSchema(fieldName, expression);
    return SapODataSchemas.annotationSchema(fieldName, expressionSchema);
  }

  // TODO nested annotations
  private Schema expressionToFieldSchema(String fieldName, EdmExpression expression) {
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
      case Path:
      case AnnotationPath:
      case LabeledElementReference:
      case Null:
      case NavigationPropertyPath:
      case PropertyPath:
        return SapODataSchemas.constantExpressionSchema(fieldName);
      case And:
        EdmAnd and = expression.asDynamic().asAnd();
        Schema andLeft = expressionToFieldSchema(fieldName, and.getLeftExpression());
        Schema andRight = expressionToFieldSchema(fieldName, and.getRightExpression());
        return SapODataSchemas.logicalExpressionSchema(fieldName + "-and", andLeft, andRight);
      case Or:
        EdmOr or = expression.asDynamic().asOr();
        Schema orLeft = expressionToFieldSchema(fieldName, or.getLeftExpression());
        Schema orRight = expressionToFieldSchema(fieldName, or.getRightExpression());
        return SapODataSchemas.logicalExpressionSchema(fieldName + "-or", orLeft, orRight);
      case Not:
        // Negation expressions are represented as an element edm:Not that MUST contain a single annotation expression.
        // See 'Comparison and Logical Operators' section of
        // https://docs.oasis-open.org/odata/odata-csdl-xml/v4.01/csprd05/odata-csdl-xml-v4.01-csprd05.html
        // However, Olingo's EdmNot interface extends common interface for logical or comparison expressions.
        // Thus, value expression can be accessed via either 'getLeftExpression' or 'getRightExpression'.
        // See: AbstractEdmLogicalOrComparisonExpression#getRightExpression implementation for details
        EdmNot not = expression.asDynamic().asNot();
        Schema value = expressionToFieldSchema(fieldName, not.getLeftExpression());
        return SapODataSchemas.notExpressionSchema(fieldName + "-not", value);
      case Eq:
        EdmEq eq = expression.asDynamic().asEq();
        Schema eqLeft = expressionToFieldSchema(fieldName, eq.getLeftExpression());
        Schema eqRight = expressionToFieldSchema(fieldName, eq.getRightExpression());
        return SapODataSchemas.logicalExpressionSchema(fieldName + "-eq", eqLeft, eqRight);
      case Ne:
        EdmNe ne = expression.asDynamic().asNe();
        Schema neLeft = expressionToFieldSchema(fieldName, ne.getLeftExpression());
        Schema neRight = expressionToFieldSchema(fieldName, ne.getRightExpression());
        return SapODataSchemas.logicalExpressionSchema(fieldName + "-ne", neLeft, neRight);
      case Gt:
        EdmGt gt = expression.asDynamic().asGt();
        Schema gtLeft = expressionToFieldSchema(fieldName, gt.getLeftExpression());
        Schema gtRight = expressionToFieldSchema(fieldName, gt.getRightExpression());
        return SapODataSchemas.logicalExpressionSchema(fieldName + "-gt", gtLeft, gtRight);
      case Ge:
        EdmGe ge = expression.asDynamic().asGe();
        Schema geLeft = expressionToFieldSchema(fieldName, ge.getLeftExpression());
        Schema geRight = expressionToFieldSchema(fieldName, ge.getRightExpression());
        return SapODataSchemas.logicalExpressionSchema(fieldName + "-ge", geLeft, geRight);
      case Lt:
        EdmLt lt = expression.asDynamic().asLt();
        Schema ltLeft = expressionToFieldSchema(fieldName, lt.getLeftExpression());
        Schema ltRight = expressionToFieldSchema(fieldName, lt.getRightExpression());
        return SapODataSchemas.logicalExpressionSchema(fieldName + "-lt", ltLeft, ltRight);
      case Le:
        EdmLe le = expression.asDynamic().asLe();
        Schema leLeft = expressionToFieldSchema(fieldName, le.getLeftExpression());
        Schema leRight = expressionToFieldSchema(fieldName, le.getRightExpression());
        return SapODataSchemas.logicalExpressionSchema(fieldName + "-le", leLeft, leRight);
      case Apply:
        EdmApply apply = expression.asDynamic().asApply();
        List<Schema.Field> parameters = apply.getParameters().stream()
          .map(e -> Schema.Field.of(e.getExpressionName(), expressionToFieldSchema(fieldName, e)))
          .collect(Collectors.toList());
        Schema parametersSchema = Schema.recordOf(fieldName + "-parameters", parameters);
        return SapODataSchemas.applyExpressionSchema(fieldName, parametersSchema);
      case Cast:
        EdmCast cast = expression.asDynamic().asCast();
        Schema expressionSchema = expressionToFieldSchema(fieldName, cast.getValue());
        return SapODataSchemas.castExpressionSchema(fieldName, expressionSchema);
      case Collection:
        EdmCollection collection = expression.asDynamic().asCollection();
        List<EdmExpression> items = collection.getItems();
        // The values of the child expressions MUST all be type compatible.
        Schema itemSchema = items == null || items.isEmpty() ? null : expressionToFieldSchema(fieldName, items.get(0));
        return SapODataSchemas.collectionExpressionSchema(fieldName, itemSchema);
      case If:
        EdmIf edmIf = expression.asDynamic().asIf();
        Schema guard = expressionToFieldSchema(fieldName, edmIf.getGuard());
        Schema then = expressionToFieldSchema(fieldName, edmIf.getThen());
        Schema elseSchema = expressionToFieldSchema(fieldName, edmIf.getElse());
        return SapODataSchemas.ifExpressionSchema(fieldName, guard, then, elseSchema);
      case IsOf:
        EdmIsOf isOf = expression.asDynamic().asIsOf();
        return SapODataSchemas.isOfExpressionSchema(fieldName, expressionToFieldSchema(fieldName, isOf.getValue()));
      case LabeledElement:
        EdmLabeledElement labeledElement = expression.asDynamic().asLabeledElement();
        Schema labeledElementSchema = expressionToFieldSchema(fieldName, labeledElement.getValue());
        return SapODataSchemas.labeledElementExpressionSchema(fieldName, labeledElementSchema);
      case Record:
        EdmRecord record = expression.asDynamic().asRecord();
        Schema propertyValuesSchema = propertyValuesSchema(fieldName, record.getPropertyValues());
        return SapODataSchemas.recordExpressionSchema(fieldName, propertyValuesSchema);
      case UrlRef:
        EdmUrlRef urlRef = expression.asDynamic().asUrlRef();
        return SapODataSchemas.urlRefExpressionSchema(fieldName, expressionToFieldSchema(fieldName, urlRef.getValue()));
      default:
        // this should never happen
        throw new InvalidStageException(
          String.format("Annotation expression for field '%s' is of unsupported type '%s'.", fieldName, type));
    }
  }

  private Schema propertyValuesSchema(String fieldName, List<EdmPropertyValue> propertyValues) {
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
      case "Binary":
        return Schema.of(Schema.Type.BYTES);
      case "Boolean":
        return Schema.of(Schema.Type.BOOLEAN);
      case "Byte":
        return Schema.of(Schema.Type.INT);
      case "SByte":
        return Schema.of(Schema.Type.INT);
      case "DateTime":
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      case "DateTimeOffset":
        // Mapped to 'string' to avoid timezone information loss
        return Schema.of(Schema.Type.STRING);
      case "Time":
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      case "Decimal":
        return Schema.decimalOf(propertyMetadata.getPrecision(), propertyMetadata.getScale());
      case "Double":
        return Schema.of(Schema.Type.DOUBLE);
      case "Single":
        return Schema.of(Schema.Type.FLOAT);
      case "Guid":
        return Schema.of(Schema.Type.STRING);
      case "Int16":
        return Schema.of(Schema.Type.INT);
      case "Int32":
        return Schema.of(Schema.Type.INT);
      case "Int64":
        return Schema.of(Schema.Type.LONG);
      case "String":
        return Schema.of(Schema.Type.STRING);
      case "GeographyPoint":
      case "GeometryPoint":
        return SapODataConstants.Point.SCHEMA;
      case "GeographyLineString":
      case "GeometryLineString":
        return SapODataConstants.LineString.SCHEMA;
      case "GeographyPolygon":
      case "GeometryPolygon":
        return SapODataConstants.Polygon.SCHEMA;
      case "GeographyMultiPoint":
      case "GeometryMultiPoint":
        return SapODataConstants.MultiPoint.SCHEMA;
      case "GeographyMultiLineString":
      case "GeometryMultiLineString":
        return SapODataConstants.MultiLineString.SCHEMA;
      case "GeographyMultiPolygon":
      case "GeometryMultiPolygon":
        return SapODataConstants.MultiPolygon.SCHEMA;
      case "GeographyCollection":
      case "GeometryCollection":
        return SapODataConstants.GeospatialCollection.SCHEMA;
      case "Date":
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      case "Duration":
        return Schema.of(Schema.Type.STRING);
      case "Stream":
        return SapODataConstants.Stream.SCHEMA;
      case "TimeOfDay":
        return Schema.of(Schema.LogicalType.TIME_MICROS);
      default:
        // this should never happen
        throw new InvalidStageException(String.format("Field '%s' is of unsupported type '%s'.",
                                                      propertyMetadata.getName(), propertyMetadata.getEdmTypeName()));
    }
  }
}
