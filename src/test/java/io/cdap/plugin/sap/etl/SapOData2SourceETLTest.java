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
package io.cdap.plugin.sap.etl;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.sap.SapODataConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

public class SapOData2SourceETLTest extends BaseSapODataSourceETLTest {

  private static final String SERVICE_PATH = "/sap/opu/odata/SAP/ZGW100_XX_S2_SRV";
  private static final String ENTITY_SET = "AllDataTypes";

  private static final Schema SCHEMA = Schema.recordOf(
    "schema",
    Schema.Field.of("Id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("Binary", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("Boolean", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("Byte", Schema.of(Schema.Type.INT)),
    Schema.Field.of("Decimal", Schema.decimalOf(16, 3)),
    Schema.Field.of("Double", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("Single", Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of("Guid", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("Int16", Schema.of(Schema.Type.INT)),
    Schema.Field.of("Int32", Schema.of(Schema.Type.INT)),
    Schema.Field.of("Int64", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("SByte", Schema.of(Schema.Type.INT)),
    Schema.Field.of("String", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("DateTime", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("Time", Schema.of(Schema.LogicalType.TIME_MICROS)),
    Schema.Field.of("DateTimeOffset", Schema.of(Schema.Type.STRING)));

  private static final Schema SCHEMA_WITH_METADATA_ANNOTATIONS = Schema.recordOf(
    "schema",
    valueWithMetadataAnnotationsField("Id", Schema.of(Schema.Type.STRING)),
    valueWithMetadataAnnotationsField("Binary", Schema.of(Schema.Type.BYTES)),
    valueWithMetadataAnnotationsField("Boolean", Schema.of(Schema.Type.BOOLEAN)),
    valueWithMetadataAnnotationsField("Byte", Schema.of(Schema.Type.INT)),
    valueWithMetadataAnnotationsField("Decimal", Schema.decimalOf(16, 3)),
    valueWithMetadataAnnotationsField("Double", Schema.of(Schema.Type.DOUBLE)),
    valueWithMetadataAnnotationsField("Single", Schema.of(Schema.Type.FLOAT)),
    valueWithMetadataAnnotationsField("Guid", Schema.of(Schema.Type.STRING)),
    valueWithMetadataAnnotationsField("Int16", Schema.of(Schema.Type.INT)),
    valueWithMetadataAnnotationsField("Int32", Schema.of(Schema.Type.INT)),
    valueWithMetadataAnnotationsField("Int64", Schema.of(Schema.Type.LONG)),
    valueWithMetadataAnnotationsField("SByte", Schema.of(Schema.Type.INT)),
    valueWithMetadataAnnotationsField("String", Schema.of(Schema.Type.STRING)),
    valueWithMetadataAnnotationsField("DateTime", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    valueWithMetadataAnnotationsField("Time", Schema.of(Schema.LogicalType.TIME_MICROS)),
    valueWithMetadataAnnotationsField("DateTimeOffset", Schema.of(Schema.Type.STRING)));

  private static Schema.Field valueWithMetadataAnnotationsField(String fieldName, Schema valueSchema) {
    Schema sapMetadataSchema = Schema.recordOf(fieldName + "-metadata",
                                               Schema.Field.of("label", Schema.of(Schema.Type.STRING)),
                                               Schema.Field.of("creatable", Schema.of(Schema.Type.STRING)),
                                               Schema.Field.of("updatable", Schema.of(Schema.Type.STRING)),
                                               Schema.Field.of("sortable", Schema.of(Schema.Type.STRING)),
                                               Schema.Field.of("filterable", Schema.of(Schema.Type.STRING)));

    return Schema.Field.of(fieldName, Schema.recordOf(
      fieldName + "-value-metadata",
      Schema.Field.of(SapODataConstants.VALUE_FIELD_NAME, valueSchema),
      Schema.Field.of(SapODataConstants.METADATA_ANNOTATIONS_FIELD_NAME, sapMetadataSchema)));
  }

  @Before
  public void testSetup() throws Exception {
    wireMockRule.stubFor(WireMock.get(WireMock.urlEqualTo(SERVICE_PATH + "/$metadata"))
                           .willReturn(WireMock.aResponse().withBody(readResourceFile("odata2/metadata.xml"))));
    wireMockRule.stubFor(WireMock.get(WireMock.urlEqualTo(SERVICE_PATH + "/" + ENTITY_SET + "?$format=json"))
                           .willReturn(WireMock.aResponse()
                                         .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                                         .withBody(readResourceFile("odata2/AllDataTypes.json"))));

    ResponseDefinitionBuilder xmlResponse = WireMock.aResponse()
      .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_XML)
      .withBody(readResourceFile("odata2/AllDataTypes.xml"));

    wireMockRule.stubFor(WireMock.get(WireMock.urlEqualTo(SERVICE_PATH + "/" + ENTITY_SET + "?$format=xml"))
                           .willReturn(xmlResponse));

    wireMockRule.stubFor(WireMock.get(WireMock.urlEqualTo(SERVICE_PATH + "/" + ENTITY_SET))
                           .willReturn(xmlResponse));
  }

  @Test
  public void testSource() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.INCLUDE_METADATA_ANNOTATIONS, "false")
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(3, records.size());
  }

  @Test
  public void testSourceXml() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.INCLUDE_METADATA_ANNOTATIONS, "false")
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.QUERY, "$format=xml")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(3, records.size());
  }

  @Test
  public void testSourceJson() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.INCLUDE_METADATA_ANNOTATIONS, "false")
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.QUERY, "$format=json")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(3, records.size());
  }

  @Test
  public void testSourceWithSchemaSet() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.INCLUDE_METADATA_ANNOTATIONS, "false")
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.SCHEMA, SCHEMA.toString())
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(3, records.size());
  }

  @Test
  public void testSourceXmlWithSchemaSet() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.INCLUDE_METADATA_ANNOTATIONS, "false")
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.SCHEMA, SCHEMA.toString())
      .put(SapODataConstants.QUERY, "$format=xml")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(3, records.size());
  }

  @Test
  public void testSourceJsonWithSchemaSet() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.INCLUDE_METADATA_ANNOTATIONS, "false")
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.SCHEMA, SCHEMA.toString())
      .put(SapODataConstants.QUERY, "$format=json")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(3, records.size());
  }

  @Test
  public void testSourceIncludeMetadataAnnotations() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.INCLUDE_METADATA_ANNOTATIONS, "true")
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(3, records.size());
    for (StructuredRecord actualRecord : records) {
      assertMetadataAnnotationsIncluded(actualRecord);
    }
  }

  @Test
  public void testSourceXmlIncludeMetadataAnnotations() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.INCLUDE_METADATA_ANNOTATIONS, "true")
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.QUERY, "$format=xml")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(3, records.size());
    for (StructuredRecord actualRecord : records) {
      assertMetadataAnnotationsIncluded(actualRecord);
    }
  }

  @Test
  public void testSourceJsonIncludeMetadataAnnotations() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.INCLUDE_METADATA_ANNOTATIONS, "true")
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.QUERY, "$format=json")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(3, records.size());
    for (StructuredRecord actualRecord : records) {
      assertMetadataAnnotationsIncluded(actualRecord);
    }
  }

  @Test
  public void testSourceIncludeMetadataAnnotationsSchemaSet() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.INCLUDE_METADATA_ANNOTATIONS, "true")
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.SCHEMA, SCHEMA_WITH_METADATA_ANNOTATIONS.toString())
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(3, records.size());
    for (StructuredRecord actualRecord : records) {
      assertMetadataAnnotationsIncluded(actualRecord);
    }
  }

  @Test
  public void testSourceXmlIncludeMetadataAnnotationsSchemaSet() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.INCLUDE_METADATA_ANNOTATIONS, "true")
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.SCHEMA, SCHEMA_WITH_METADATA_ANNOTATIONS.toString())
      .put(SapODataConstants.QUERY, "$format=xml")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(3, records.size());
    for (StructuredRecord actualRecord : records) {
      assertMetadataAnnotationsIncluded(actualRecord);
    }
  }

  @Test
  public void testSourceJsonIncludeMetadataAnnotationsSchemaSet() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.INCLUDE_METADATA_ANNOTATIONS, "true")
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.SCHEMA, SCHEMA_WITH_METADATA_ANNOTATIONS.toString())
      .put(SapODataConstants.QUERY, "$format=json")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(3, records.size());
    for (StructuredRecord actualRecord : records) {
      assertMetadataAnnotationsIncluded(actualRecord);
    }
  }

  private void assertMetadataAnnotationsIncluded(StructuredRecord actualRecord) {
    List<Schema.Field> actualFields = actualRecord.getSchema().getFields();
    Assert.assertNotNull(actualFields);
    for (Schema.Field actualField : actualFields) {
      Assert.assertEquals(Schema.Type.RECORD, actualField.getSchema().getType());
      StructuredRecord annotatedValueRecord = actualRecord.get(actualField.getName());

      Schema annotatedValueSchema = annotatedValueRecord.getSchema();
      Schema.Field metadataField = annotatedValueSchema.getField(SapODataConstants.METADATA_ANNOTATIONS_FIELD_NAME);
      Assert.assertNotNull(metadataField);
      Assert.assertEquals(metadataField.getSchema().getType(), Schema.Type.RECORD);
      StructuredRecord annotations = annotatedValueRecord.get(metadataField.getName());
      Assert.assertEquals("false", annotations.get("creatable"));
      Assert.assertEquals("false", annotations.get("updatable"));
      Assert.assertEquals("false", annotations.get("sortable"));
      Assert.assertEquals("false", annotations.get("filterable"));
    }
  }
}
