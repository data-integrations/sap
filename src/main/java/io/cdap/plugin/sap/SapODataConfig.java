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

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.IdUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Defines a {@link PluginConfig} that {@link SapODataSource} can use.
 */
public class SapODataConfig extends PluginConfig {

  private static final String QUESTION_MARK = "?";
  private static final String QUERY_OPTIONS_DELIMITER = "&";
  private static final String QUERY_OPTIONS_NAME_VALUE_DELIMITER = "=";

  private static final Set<Schema.Type> SUPPORTED_SIMPLE_TYPES = ImmutableSet.of(Schema.Type.BOOLEAN, Schema.Type.INT,
                                                                                 Schema.Type.FLOAT, Schema.Type.DOUBLE,
                                                                                 Schema.Type.BYTES, Schema.Type.LONG,
                                                                                 Schema.Type.STRING);

  private static final Set<Schema.LogicalType> SUPPORTED_LOGICAL_TYPES = ImmutableSet.of(
    Schema.LogicalType.DECIMAL, Schema.LogicalType.TIMESTAMP_MILLIS, Schema.LogicalType.TIMESTAMP_MICROS,
    Schema.LogicalType.TIME_MILLIS, Schema.LogicalType.TIME_MICROS);

  @Name(Constants.Reference.REFERENCE_NAME)
  @Description(Constants.Reference.REFERENCE_NAME_DESCRIPTION)
  private String referenceName;

  @Name(SapODataConstants.ODATA_SERVICE_URL)
  @Description("Root URL of the SAP OData service.")
  @Macro
  private String url;

  @Name(SapODataConstants.RESOURCE_PATH)
  @Description("Path of the SAP OData entity.")
  @Macro
  private String resourcePath;

  @Name(SapODataConstants.QUERY)
  @Description("OData query options to filter the data.")
  @Macro
  @Nullable
  private String query;

  @Name(SapODataConstants.USERNAME)
  @Description("Username for basic authentication.")
  @Macro
  @Nullable
  private String user;

  @Name(SapODataConstants.PASSWORD)
  @Description("Password for basic authentication.")
  @Macro
  @Nullable
  private String password;

  @Name(SapODataConstants.SCHEMA)
  @Description("Schema of records output by the source.")
  @Nullable
  private String schema;

  public SapODataConfig(String referenceName, String url, String resourcePath, String query, String user,
                        String password, String schema) {
    this.referenceName = referenceName;
    this.url = url;
    this.resourcePath = resourcePath;
    this.query = query;
    this.user = user;
    this.password = password;
    this.schema = schema;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public String getUrl() {
    return url;
  }

  public String getResourcePath() {
    return resourcePath;
  }

  /**
   * Returns OData query. All leading question marks will be removed.
   * For example, for the user-provided query "???$top=2&$select=By?yerName,Surnam?"
   * the following query will be returned: "$top=2&$select=By?yerName,Surnam?".
   */
  @Nullable
  public String getQuery() {
    if (Strings.isNullOrEmpty(query)) {
      return query;
    }

    if (query.startsWith(QUESTION_MARK)) {
      // remove all leading question marks
      return query.replaceAll("^\\?+", "");
    }

    return query;
  }

  /**
   * An OData query can contain '$select' option. The $select option specifies a subset of properties to include in the
   * response body. For example, to get only the name and price of each product, the following query can be used:
   * 'http://localhost/odata/Products?$select=Price,Name'.
   *
   * @return empty list if no '$select' query option specified. List of the property names to include, otherwise(order
   * is preserved).
   */
  public List<String> getSelectProperties() {
    if (Strings.isNullOrEmpty(query) || !query.contains("$select")) {
      return Collections.emptyList();
    }
    // There is no straightforward way to parse $select query options using Olingo V2
    String selectOption = "$select=";
    int start = query.indexOf(selectOption) + selectOption.length();
    int end = query.indexOf("&", start);
    String commaSeparatedPropertyNames = end != -1 ? query.substring(start, end) : query.substring(start);

    return Arrays.asList(commaSeparatedPropertyNames.split(","));
  }

  @Nullable
  public String getUser() {
    return user;
  }

  @Nullable
  public String getPassword() {
    return password;
  }

  @Nullable
  public String getSchema() {
    return schema;
  }

  /**
   * Parses the json representation into a schema object.
   *
   * @return parsed schema object of json representation.
   * @throws RuntimeException if there was an exception parsing the schema.
   */
  @Nullable
  public Schema getParsedSchema() {
    try {
      return schema == null ? null : Schema.parseJson(schema);
    } catch (IOException e) {
      // this should not happen, since schema string comes from UI
      throw Throwables.propagate(e);
    }
  }

  /**
   * Validates {@link SapODataConfig} instance.
   *
   * @param collector failure collector.
   */
  public void validate(FailureCollector collector) {
    if (Strings.isNullOrEmpty(referenceName)) {
      collector.addFailure("Reference name must be specified", null)
        .withConfigProperty(Constants.Reference.REFERENCE_NAME);
    } else {
      try {
        IdUtils.validateId(referenceName);
      } catch (IllegalArgumentException e) {
        collector.addFailure("Invalid reference name", "Change the reference name to only " +
          "include letters, numbers, periods, underscores, or dashes.")
          .withConfigProperty(Constants.Reference.REFERENCE_NAME);
      }
    }
    if (!containsMacro(SapODataConstants.ODATA_SERVICE_URL) && Strings.isNullOrEmpty(url)) {
      collector.addFailure("OData Service URL must be specified", "Specify valid OData Service URL")
        .withConfigProperty(SapODataConstants.ODATA_SERVICE_URL);
    }
    if (!containsMacro(SapODataConstants.RESOURCE_PATH) && Strings.isNullOrEmpty(resourcePath)) {
      collector.addFailure("Resource path must be specified", "Specify valid resource path")
        .withConfigProperty(SapODataConstants.RESOURCE_PATH);
    }

    if (!Strings.isNullOrEmpty(schema) && !containsMacro(SapODataConstants.SCHEMA)) {
      Schema parsedSchema = getParsedSchema();
      validateSchema(parsedSchema, collector);
    }

    collector.getOrThrowException();
  }

  private void validateSchema(Schema parsedSchema, FailureCollector collector) {
    List<Schema.Field> fields = parsedSchema.getFields();
    if (null == fields || fields.isEmpty()) {
      collector.addFailure("Schema must contain at least one field", null)
        .withConfigProperty(SapODataConstants.SCHEMA);
      collector.getOrThrowException();
    }
    for (Schema.Field field : fields) {
      Schema nonNullableSchema = field.getSchema().isNullable() ?
        field.getSchema().getNonNullable() : field.getSchema();
      Schema.Type type = nonNullableSchema.getType();
      Schema.LogicalType logicalType = nonNullableSchema.getLogicalType();
      if (!SUPPORTED_SIMPLE_TYPES.contains(type) && !SUPPORTED_LOGICAL_TYPES.contains(logicalType)) {
        String supportedTypeNames = Stream.concat(
          SUPPORTED_SIMPLE_TYPES.stream().map(Enum::name).map(String::toLowerCase),
          SUPPORTED_LOGICAL_TYPES.stream().map(Schema.LogicalType::getToken)
        ).collect(Collectors.joining(", "));
        String errorMessage = String.format("Field '%s' is of unsupported type '%s'. Supported types are: %s",
                                            field.getName(), nonNullableSchema.getDisplayName(), supportedTypeNames);
        collector.addFailure(errorMessage, String.format("Change field '%s' to be a supported type", field.getName()))
          .withOutputSchemaField(field.getName(), null);
      }
    }
  }

  /**
   * Validate that the provided schema is compatible with the inferred schema. The provided schema is compatible if
   * every field is compatible with the corresponding field in the inferred schema. A field is compatible if it is of
   * the same type or is a nullable version of that type. It is assumed that both schemas are record schemas.
   *
   * @param inferredSchema the inferred schema
   * @param providedSchema the provided schema to check compatibility
   * @param collector      failure collector
   * @throws IllegalArgumentException if the schemas are not type compatible
   */
  public static void validateFieldsMatch(Schema inferredSchema, Schema providedSchema, FailureCollector collector) {
    for (Schema.Field field : providedSchema.getFields()) {
      Schema.Field inferredField = inferredSchema.getField(field.getName());
      if (inferredField == null) {
        String errorMessage = String.format("Field '%s' does not exist in SAP", field.getName());
        collector.addFailure(errorMessage, String.format("Remove field '%s' from the output schema", field.getName()))
          .withOutputSchemaField(field.getName(), null);
      }
      Schema inferredFieldSchema = inferredField.getSchema();
      Schema providedFieldSchema = field.getSchema();

      boolean isInferredFieldNullable = inferredFieldSchema.isNullable();
      boolean isProvidedFieldNullable = providedFieldSchema.isNullable();

      Schema inferredFieldNonNullableSchema = isInferredFieldNullable
        ? inferredFieldSchema.getNonNullable() : inferredFieldSchema;
      Schema providedFieldNonNullableSchema = isProvidedFieldNullable ?
        providedFieldSchema.getNonNullable() : providedFieldSchema;

      if (inferredFieldNonNullableSchema.getType() != providedFieldNonNullableSchema.getType() &&
        inferredFieldNonNullableSchema.getLogicalType() != providedFieldNonNullableSchema.getLogicalType()) {
        String errorMessage = String.format("Expected field '%s' to be of type '%s', but it is of type '%s'",
                                            field.getName(), inferredFieldNonNullableSchema.getDisplayName(),
                                            providedFieldNonNullableSchema.getDisplayName());

        collector.addFailure(errorMessage, String.format("Change field '%s' to be a supported type", field.getName()))
          .withOutputSchemaField(field.getName(), null);
      }

      if (!isInferredFieldNullable && isProvidedFieldNullable) {
        String errorMessage = String.format("Field '%s' should not be nullable", field.getName());
        collector.addFailure(errorMessage, String.format("Change field '%s' to be non-nullable", field.getName()))
          .withOutputSchemaField(field.getName(), null);
      }
    }
    collector.getOrThrowException();
  }
}
