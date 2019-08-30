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

package io.cdap.plugin.sap.odata;

import io.cdap.plugin.sap.odata.exception.ODataException;
import io.cdap.plugin.sap.odata.odata2.OData2Client;
import io.cdap.plugin.sap.odata.odata4.OData4Client;
import org.apache.olingo.client.api.communication.request.retrieve.XMLMetadataRequest;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.edm.xml.XMLMetadata;
import org.apache.olingo.client.core.ODataClientFactory;
import org.apache.olingo.client.core.http.BasicAuthHttpClientFactory;

import java.util.Iterator;
import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;

/**
 * Provides handy methods to consume OData v2 and OData v4 services transparently using Apache Olingo Client Libraries.
 */
public class GenericODataClient extends ODataClient {

  private ODataClient oDataClient;

  /**
   * @param rootUrl  URL of the OData service. The URL must end with an external service name
   *                 (e.g., http://eccsvrname:8000/sap/opu/odata/sap/zgw100_dd02l_so_srv/).
   * @param username username for basic authentication.
   * @param password password for basic authentication.
   */
  public GenericODataClient(String rootUrl, String username, String password) {
    super(rootUrl, username, password);
  }

  @Override
  public Iterator<ODataEntity> queryEntitySet(String entitySetName, @Nullable String query) {
    return getClient().queryEntitySet(entitySetName, query);
  }

  @Override
  public EntityType getEntitySetType(String entitySetName) {
    return getClient().getEntitySetType(entitySetName);
  }

  private ODataClient getClient() {
    if (oDataClient == null) {
      initClient();
    }
    return oDataClient;
  }

  private void initClient() {
    String edmVersion = getEdmVersion();
    ODataVersion version = ODataVersion.fromEdmVersion(edmVersion);
    if (version == null) {
      throw new ODataException(String.format("Unsupported EDM version: '%s'.", edmVersion));
    }
    switch (version) {
      case V2:
        oDataClient = new OData2Client(rootUrl, username, password);
        break;
      case V4:
        oDataClient = new OData4Client(rootUrl, username, password);
        break;
    }
  }

  private String getEdmVersion() {
    org.apache.olingo.client.api.ODataClient client = ODataClientFactory.getClient();
    if (isAuthRequired()) {
      client.getConfiguration().setHttpClientFactory(new BasicAuthHttpClientFactory(username, password));
    }
    XMLMetadataRequest request = client.getRetrieveRequestFactory().getXMLMetadataRequest(rootUrl);
    request.setAccept(MediaType.APPLICATION_XML);
    ODataRetrieveResponse<XMLMetadata> response = request.execute();

    return response.getBody().getEdmVersion();
  }
}
