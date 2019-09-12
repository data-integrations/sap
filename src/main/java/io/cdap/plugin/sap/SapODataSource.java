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
import io.cdap.plugin.sap.exception.ODataException;
import org.apache.hadoop.io.NullWritable;
import org.apache.olingo.odata2.api.edm.EdmEntityType;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.edm.EdmTyped;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Plugin returns records from SAP OData service specified by URL.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SapODataConstants.PLUGIN_NAME)
@Description("Read data from SAP OData service.")
public class SapODataSource extends BatchSource<NullWritable, ODataEntry, StructuredRecord> {

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
      new OData2Client(config.getUrl(), config.getUser(), config.getPassword())
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
      new OData2Client(config.getUrl(), config.getUser(), config.getPassword())
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
    this.transformer = new ODataEntryToRecordTransformer(schema);
  }

  @Override
  public void transform(KeyValue<NullWritable, ODataEntry> input, Emitter<StructuredRecord> emitter) {
    ODataEntry entity = input.getValue();
    emitter.emit(transformer.transform(entity));
  }

  public Schema getSchema() {
    OData2Client oData2Client = new OData2Client(config.getUrl(), config.getUser(), config.getPassword());
    try {
      EdmEntityType edmEntityType = oData2Client.getEntitySetType(config.getResourcePath());
      List<String> selectProperties = config.getSelectProperties();
      List<Schema.Field> fields = new ArrayList<>();
      for (String propertyName : edmEntityType.getPropertyNames()) {
        if (!selectProperties.isEmpty() && !selectProperties.contains(propertyName)) {
          continue;
        }
        EdmTyped property = edmEntityType.getProperty(propertyName);
        fields.add(getSchemaField(property));
      }
      return Schema.recordOf("output", fields);
    } catch (EdmException | ODataException e) {
      throw new InvalidStageException("Unable to get details about the entity type: " + e.getMessage(), e);
    }
  }

  private Schema.Field getSchemaField(EdmTyped edmTyped) throws EdmException {
    Schema nonNullableSchema = convertPropertyType(edmTyped);
    EdmProperty property = (EdmProperty) edmTyped;
    Schema schema = property.getFacets().isNullable() ? Schema.nullableOf(nonNullableSchema) : nonNullableSchema;
    return Schema.Field.of(edmTyped.getName(), schema);
  }

  private Schema convertPropertyType(EdmTyped edmTyped) throws EdmException {
    switch (edmTyped.getType().getName()) {
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
        EdmProperty property = (EdmProperty) edmTyped;
        return Schema.decimalOf(property.getFacets().getPrecision(), property.getFacets().getScale());
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
      default:
        // this should never happen
        throw new InvalidStageException(String.format("Field '%s' is of unsupported type '%s'.", edmTyped.getName(),
                                                      edmTyped.getType().getName()));
    }
  }
}
