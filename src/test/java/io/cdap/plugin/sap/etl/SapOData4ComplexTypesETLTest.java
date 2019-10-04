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


public class SapOData4ComplexTypesETLTest extends BaseSapODataSourceETLTest {

  private static final String SERVICE_PATH = "/sap/opu/odata/SAP/ZGW100_XX_S2_SRV";
  private static final String ENTITY_SET = "AllDataTypes";

  @Before
  public void testSetup() throws Exception {
    wireMockRule.stubFor(WireMock.get(WireMock.urlEqualTo(SERVICE_PATH + "/$metadata"))
                           .willReturn(WireMock.aResponse().withBody(readResourceFile("odata4/metadata.xml"))));

    ResponseDefinitionBuilder jsonResponse = WireMock.aResponse()
      .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
      .withBody(readResourceFile("odata4/AllDataTypes.json"));
    wireMockRule.stubFor(WireMock.get(WireMock.urlMatching(SERVICE_PATH + "/" + ENTITY_SET + ".*"))
                           .willReturn(jsonResponse));
  }

  @Test
  public void testComplexType() throws Exception {
    Map<String, String> properties = sourceProperties("$select=Address");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("Address");
      // todo
    }
  }

  @Test
  public void testComplexTypeWithBaseType() throws Exception {
    Map<String, String> properties = sourceProperties("$select=HomeAddress");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("HomeAddress");
      // todo
    }
  }

  @Test
  public void testComplexTypeWithTypeDefinitions() throws Exception {
    Map<String, String> properties = sourceProperties("$select=Size");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("Size");
      // todo
    }
  }

  @Test
  public void testEnumType() throws Exception {
    Map<String, String> properties = sourceProperties("$select=Gender");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      String gender = actualRecord.get("Gender");
      Assert.assertEquals("Male", gender);
    }
  }

  // todo include metadata annotations

  private Map<String, String> sourceProperties(String query) {
    return new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.INCLUDE_METADATA_ANNOTATIONS, "false")
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.QUERY, query)
      .build();
  }
}
