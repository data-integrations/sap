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
import io.cdap.plugin.sap.SapODataConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

/**
 * Complex types tests for both OData V2 and V4.
 */
public class ComplexTypesETLTest extends BaseSapODataSourceETLTest {

  private static final String ODATA_2_SERVICE_PATH = "/sap/opu/odata/SAP/ZGW100_XX_S2_SRV";
  private static final String ODATA_4_SERVICE_PATH = "/sap/opu/odata/SAP/ZGW100_XX_S4_SRV";
  private static final String ENTITY_SET = "AllDataTypes";

  @Before
  public void testSetup() throws Exception {
    wireMockRule.stubFor(WireMock.get(WireMock.urlEqualTo(ODATA_2_SERVICE_PATH + "/$metadata"))
                           .willReturn(WireMock.aResponse().withBody(readResourceFile("odata2/metadata.xml"))));
    wireMockRule.stubFor(WireMock.get(WireMock.urlEqualTo(ODATA_4_SERVICE_PATH + "/$metadata"))
                           .willReturn(WireMock.aResponse().withBody(readResourceFile("odata4/metadata.xml"))));

    ResponseDefinitionBuilder oData2JsonResponse = WireMock.aResponse()
      .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
      .withBody(readResourceFile("odata2/AllDataTypes.json"));
    wireMockRule.stubFor(WireMock.get(WireMock.urlMatching(ODATA_2_SERVICE_PATH + "/" + ENTITY_SET + ".*"))
                           .willReturn(oData2JsonResponse));
    ResponseDefinitionBuilder jsonResponse = WireMock.aResponse()
      .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
      .withBody(readResourceFile("odata4/AllDataTypes.json"));
    wireMockRule.stubFor(WireMock.get(WireMock.urlMatching(ODATA_4_SERVICE_PATH + "/" + ENTITY_SET + ".*"))
                           .willReturn(jsonResponse));
  }

  @Test
  public void testOData2ComplexType() throws Exception {
    testComplexType(sourceProperties(ODATA_2_SERVICE_PATH, "$select=Address"));
  }

  @Test
  public void testOData4ComplexType() throws Exception {
    testComplexType(sourceProperties(ODATA_4_SERVICE_PATH, "$select=Address"));
  }

  @Test
  public void testOData2ComplexTypeWithBaseType() throws Exception {
    testComplexTypeWithBaseType(sourceProperties(ODATA_2_SERVICE_PATH, "$select=HomeAddress"));
  }

  @Test
  public void testOData4ComplexTypeWithBaseType() throws Exception {
    testComplexTypeWithBaseType(sourceProperties(ODATA_4_SERVICE_PATH, "$select=HomeAddress"));
  }

  @Test
  public void testOData4ComplexTypeWithTypeDefinitions() throws Exception {
    Map<String, String> properties = sourceProperties(ODATA_4_SERVICE_PATH, "$select=Size");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord size = actualRecord.get("Size");
      Assert.assertEquals(12, (int) size.get("Height"));
      Assert.assertEquals(10, (int) size.get("Weight"));
    }
  }

  @Test
  public void testOData4EnumType() throws Exception {
    Map<String, String> properties = sourceProperties(ODATA_4_SERVICE_PATH, "$select=Gender");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      String gender = actualRecord.get("Gender");
      Assert.assertEquals("Male", gender);
    }
  }

  private void testComplexType(Map<String, String> properties) throws Exception {
    List<StructuredRecord> records = getPipelineResults(properties);
    for (StructuredRecord actualRecord : records) {
      StructuredRecord address = actualRecord.get("Address");
      Assert.assertEquals("187 Suffolk Ln.", address.get("Address"));
      StructuredRecord city = address.get("City");
      Assert.assertNotNull(city);
      Assert.assertEquals("United States", city.get("CountryRegion"));
      Assert.assertEquals("ID", city.get("Region"));
    }
  }

  private void testComplexTypeWithBaseType(Map<String, String> properties) throws Exception {
    List<StructuredRecord> records = getPipelineResults(properties);
    for (StructuredRecord actualRecord : records) {
      StructuredRecord address = actualRecord.get("HomeAddress");
      Assert.assertEquals("Test", address.get("FamilyName"));
      Assert.assertEquals("187 Suffolk Ln.", address.get("Address"));
      StructuredRecord city = address.get("City");
      Assert.assertNotNull(city);
      Assert.assertEquals("United States", city.get("CountryRegion"));
      Assert.assertEquals("ID", city.get("Region"));
    }
  }

  private Map<String, String> sourceProperties(String servicePath, String query) {
    return new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + servicePath)
      .put(SapODataConstants.INCLUDE_METADATA_ANNOTATIONS, "false")
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.QUERY, query)
      .build();
  }
}
