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

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.common.Constants;
import org.apache.olingo.odata2.core.rt.RuntimeDelegateImpl;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

public class SapODataSourceETLTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @Rule
  public TestName name = new TestName();

  @Rule
  public WireMockRule wireMockRule = new WireMockRule();

  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the artifact as its parent.
    // this will make our plugins available.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"), parentArtifact,
                      SapODataSource.class, RuntimeDelegateImpl.class);
  }

  @Test
  public void testSource() throws Exception {
    testSource(null);
  }

  @Test
  public void testSourceWithSchemaSet() throws Exception {
    Schema schema = Schema.recordOf("schema",
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

    testSource(schema);
  }

  public void testSource(@Nullable Schema schema) throws Exception {
    ImmutableMap.Builder<String, String> properties = new ImmutableMap.Builder<String, String>()
      // http://vhcalnplci.dummy.nodomain:8000/sap/opu/odata/SAP/ZGW100_XX_S2_SRV/
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + "/sap/opu/odata/SAP/ZGW100_XX_S2_SRV")
      .put(SapODataConstants.RESOURCE_PATH, "AllDataTypes");
    if (schema != null) {
      properties.put(SapODataConstants.SCHEMA, schema.toString());
    }

    wireMockRule.stubFor(WireMock.get(WireMock.urlEqualTo("/sap/opu/odata/SAP/ZGW100_XX_S2_SRV/$metadata"))
                           .willReturn(WireMock.aResponse().withBody(readResourceFile("metadata.xml"))));
    wireMockRule.stubFor(
      WireMock.get(WireMock.urlEqualTo("/sap/opu/odata/SAP/ZGW100_XX_S2_SRV/AllDataTypes"))
        .willReturn(WireMock.aResponse()
                      .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_XML)
                      .withBody(readResourceFile("AllDataTypes.xml"))));

    List<StructuredRecord> records = getPipelineResults(properties.build());
    Assert.assertEquals(3, records.size());
  }

  public List<StructuredRecord> getPipelineResults(Map<String, String> sourceProperties) throws Exception {
    Map<String, String> allProperties = new ImmutableMap.Builder<String, String>()
      .put(Constants.Reference.REFERENCE_NAME, name.getMethodName())
      .putAll(sourceProperties)
      .build();

    ETLStage source = new ETLStage("SapODataSource", new ETLPlugin(SapODataConstants.PLUGIN_NAME,
                                                                   BatchSource.PLUGIN_TYPE, allProperties, null));

    String outputDatasetName = "output-batchsourcetest_" + name.getMethodName();
    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(outputDatasetName));

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    ApplicationId pipelineId = NamespaceId.DEFAULT.app("Sap_" + name.getMethodName());
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, etlConfig));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    return MockSink.readOutput(outputManager);
  }

  protected String readResourceFile(String filename) throws URISyntaxException, IOException {
    return new String(Files.readAllBytes(
      Paths.get(getClass().getClassLoader().getResource(filename).toURI())));
  }

  protected String getServerAddress() {
    return "http://localhost:" + wireMockRule.port();
  }
}
