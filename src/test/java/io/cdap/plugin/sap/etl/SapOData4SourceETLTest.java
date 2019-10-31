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

public class SapOData4SourceETLTest extends BaseSapODataSourceETLTest {

  private static final String SERVICE_PATH = "/sap/opu/odata/SAP/ZGW100_XX_S2_SRV";
  private static final String ENTITY_SET = "AllDataTypes";

  private static final Schema SCHEMA = Schema.recordOf(
    "schema",
    Schema.Field.of("Binary", Schema.of(Schema.Type.BYTES)),
    Schema.Field.of("Boolean", Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of("Byte", Schema.of(Schema.Type.INT)),
    Schema.Field.of("Date", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    Schema.Field.of("DateTimeOffset", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("Decimal", Schema.decimalOf(16, 3)),
    Schema.Field.of("Double", Schema.of(Schema.Type.DOUBLE)),
    Schema.Field.of("Duration", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("Guid", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("Int16", Schema.of(Schema.Type.INT)),
    Schema.Field.of("Int32", Schema.of(Schema.Type.INT)),
    Schema.Field.of("Int64", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("SByte", Schema.of(Schema.Type.INT)),
    Schema.Field.of("Single", Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of("Stream", SapODataConstants.Stream.SCHEMA),
    Schema.Field.of("Single", Schema.of(Schema.Type.FLOAT)),
    Schema.Field.of("String", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("TimeOfDay", Schema.of(Schema.LogicalType.TIME_MICROS)),
    Schema.Field.of("GeographyPoint", SapODataConstants.Point.SCHEMA),
    Schema.Field.of("GeographyLineString", SapODataConstants.LineString.SCHEMA),
    Schema.Field.of("GeographyPolygon", SapODataConstants.Polygon.SCHEMA),
    Schema.Field.of("GeographyMultiPoint", SapODataConstants.MultiPoint.SCHEMA),
    Schema.Field.of("GeographyMultiLineString", SapODataConstants.MultiLineString.SCHEMA),
    Schema.Field.of("GeographyMultiPolygon", SapODataConstants.MultiPolygon.SCHEMA),
    Schema.Field.of("GeographyCollection", SapODataConstants.GeospatialCollection.SCHEMA),
    Schema.Field.of("GeometryPoint", SapODataConstants.Point.SCHEMA),
    Schema.Field.of("GeometryLineString", SapODataConstants.LineString.SCHEMA),
    Schema.Field.of("GeometryPolygon", SapODataConstants.Polygon.SCHEMA),
    Schema.Field.of("GeometryMultiPoint", SapODataConstants.MultiPoint.SCHEMA),
    Schema.Field.of("GeometryMultiLineString", SapODataConstants.MultiLineString.SCHEMA),
    Schema.Field.of("GeometryMultiPolygon", SapODataConstants.MultiPolygon.SCHEMA),
    Schema.Field.of("GeometryCollection", SapODataConstants.GeospatialCollection.SCHEMA)
  );

  @Before
  public void testSetup() throws Exception {
    wireMockRule.stubFor(WireMock.get(WireMock.urlEqualTo(SERVICE_PATH + "/$metadata"))
                           .willReturn(WireMock.aResponse().withBody(readResourceFile("odata4/metadata.xml"))));
    wireMockRule.stubFor(WireMock.get(WireMock.urlEqualTo(SERVICE_PATH + "/$metadata#AllDataTypes"))
                           .willReturn(WireMock.aResponse().withBody(readResourceFile("odata4/metadata.xml"))));

    wireMockRule.stubFor(WireMock.get(WireMock.urlEqualTo(SERVICE_PATH + "/" + ENTITY_SET + "?$format=xml"))
                           .willReturn(WireMock.aResponse()
                                         .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_ATOM_XML)
                                         .withBody(readResourceFile("odata4/AllDataTypes.xml"))));

    ResponseDefinitionBuilder jsonResponse = WireMock.aResponse()
      .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
      .withBody(readResourceFile("odata4/AllDataTypes.json"));
    wireMockRule.stubFor(WireMock.get(WireMock.urlEqualTo(SERVICE_PATH + "/" + ENTITY_SET + "?$format=json"))
                           .willReturn(jsonResponse));

    wireMockRule.stubFor(WireMock.get(WireMock.urlEqualTo(SERVICE_PATH + "/" + ENTITY_SET))
                           .willReturn(jsonResponse));
  }

  @Test
  public void testSource() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
  }

  @Test
  public void testSourceJson() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.QUERY, "$format=json")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
  }

  @Test
  public void testSourceXml() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.QUERY, "$format=xml")
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
  }

  @Test
  public void testSourceWithSchemaSet() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.SCHEMA, SCHEMA.toString())
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
  }

  @Test
  public void testSourceJsonWithSchemaSet() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.QUERY, "$format=json")
      .put(SapODataConstants.SCHEMA, SCHEMA.toString())
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
  }

  @Test
  public void testSourceXmlWithSchemaSet() throws Exception {
    Map<String, String> properties = new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.QUERY, "$format=xml")
      .put(SapODataConstants.SCHEMA, SCHEMA.toString())
      .build();

    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
  }
}
