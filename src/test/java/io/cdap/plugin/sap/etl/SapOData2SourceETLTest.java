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
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(3, records.size());
  }

  @Test
  public void testSourceXml() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
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
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.SCHEMA, SCHEMA.toString())
      .put(SapODataConstants.QUERY, "$format=json")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(3, records.size());
  }
}
