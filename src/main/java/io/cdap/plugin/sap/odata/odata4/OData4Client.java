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

package io.cdap.plugin.sap.odata.odata4;

import io.cdap.plugin.sap.odata.EntityType;
import io.cdap.plugin.sap.odata.ODataAnnotation;
import io.cdap.plugin.sap.odata.ODataClient;
import io.cdap.plugin.sap.odata.ODataEntity;
import io.cdap.plugin.sap.odata.PropertyMetadata;
import io.cdap.plugin.sap.odata.exception.ODataException;
import org.apache.olingo.client.api.communication.request.retrieve.EdmMetadataRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataEntitySetIteratorRequest;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.apache.olingo.client.api.domain.ClientEntitySetIterator;
import org.apache.olingo.client.core.ODataClientFactory;
import org.apache.olingo.client.core.http.BasicAuthHttpClientFactory;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmAnnotation;
import org.apache.olingo.commons.api.edm.EdmEntitySet;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmProperty;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;

/**
 * Provides handy methods to consume OData v4 services using Apache Olingo v4 as Client Library.
 */
public class OData4Client extends ODataClient {

  private org.apache.olingo.client.api.ODataClient client;

  /**
   * @param rootUrl  URL of the OData service. The URL must end with an external service name
   *                 (e.g., http://eccsvrname:8000/sap/opu/odata/sap/zgw100_dd02l_so_srv/).
   * @param username username for basic authentication.
   * @param password password for basic authentication.
   */
  public OData4Client(String rootUrl, String username, String password) {
    super(rootUrl, username, password);
    this.client = ODataClientFactory.getClient();
    if (isAuthRequired()) {
      client.getConfiguration().setHttpClientFactory(new BasicAuthHttpClientFactory(username, password));
    }
  }

  @Override
  public Iterator<ODataEntity> queryEntitySet(String entitySetName, @Nullable String query) {
    URI queryURI = getQueryURI(entitySetName, query);
    ODataEntitySetIteratorRequest<ClientEntitySet, ClientEntity> request = client
      .getRetrieveRequestFactory()
      .getEntitySetIteratorRequest(queryURI);
    request.setAccept(MediaType.APPLICATION_JSON);

    ODataRetrieveResponse<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> response = request.execute();
    return new OData4EntityIterator(response.getBody());
  }

  @Override
  public EntityType getEntitySetType(String entitySetName) {
    // https://issues.apache.org/jira/browse/OLINGO-1130
    EdmMetadataRequest request = client.getRetrieveRequestFactory().getMetadataRequest(rootUrl);
    request.setAccept(MediaType.APPLICATION_XML);

    ODataRetrieveResponse<Edm> response = request.execute();
    Edm edm = response.getBody();
    EdmEntitySet entitySet = edm.getEntityContainer().getEntitySet(entitySetName);
    if (entitySet == null) {
      throw new ODataException(String.format("Entity set '%s' does not exist in SAP", entitySetName));
    }
    EdmEntityType entityType = entitySet.getEntityType();
    List<PropertyMetadata> properties = new ArrayList<>();
    for (String propertyName : entityType.getPropertyNames()) {
      EdmProperty property = (EdmProperty) entityType.getProperty(propertyName);
      properties.add(edmToProperty(property));
    }

    return new EntityType(entityType.getName(), properties);
  }

  private PropertyMetadata edmToProperty(EdmProperty property) {
    String type = property.getType().getName();
    boolean nullable = property.isNullable();
    Integer precision = property.getPrecision();
    Integer scale = property.getScale();
    List<EdmAnnotation> edmAnnotations = property.getAnnotations();
    if (edmAnnotations == null || edmAnnotations.isEmpty()) {
      return new PropertyMetadata(property.getName(), type, nullable, precision, scale, null);
    }

    // include metadata annotations
    List<ODataAnnotation> annotations = edmAnnotations.stream()
      .map(OData4Annotation::new)
      .collect(Collectors.toList());

    return new PropertyMetadata(property.getName(), type, nullable, precision, scale, annotations);
  }
}
