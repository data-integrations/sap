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
import org.apache.olingo.client.api.communication.request.retrieve.ODataEntitySetIteratorRequest;
import org.apache.olingo.client.api.communication.request.retrieve.XMLMetadataRequest;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.apache.olingo.client.api.domain.ClientEntitySetIterator;
import org.apache.olingo.client.api.edm.xml.XMLMetadata;
import org.apache.olingo.client.core.ODataClientFactory;
import org.apache.olingo.client.core.http.BasicAuthHttpClientFactory;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlAnnotation;
import org.apache.olingo.commons.api.edm.provider.CsdlAnnotations;
import org.apache.olingo.commons.api.edm.provider.CsdlEntitySet;
import org.apache.olingo.commons.api.edm.provider.CsdlEntityType;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
    // Use XML metadata request instead of metadata request, since it's not possible to fetch property annotations in
    // the case of missing vocabulary references: https://issues.apache.org/jira/browse/OLINGO-1130
    XMLMetadataRequest request = client.getRetrieveRequestFactory().getXMLMetadataRequest(rootUrl);
    request.setAccept(MediaType.APPLICATION_XML);

    ODataRetrieveResponse<XMLMetadata> response = request.execute();
    XMLMetadata metadata = response.getBody();
    CsdlSchema schema = metadata.getSchemas().stream()
      .filter(s -> s.getEntityContainer() != null && s.getEntityContainer().getEntitySet(entitySetName) != null)
      .findFirst()
      .orElseThrow(() -> new ODataException(String.format("Entity set '%s' does not exist in SAP", entitySetName)));
    CsdlEntitySet entitySet = schema.getEntityContainer().getEntitySet(entitySetName);
    FullQualifiedName entityTypeFQN = entitySet.getTypeFQN();
    CsdlEntityType entityType = metadata.getSchema(entityTypeFQN.getNamespace()).getEntityType(entityTypeFQN.getName());

    String typeName = entityTypeFQN.getName();
    Map<String, List<CsdlAnnotation>> externalTargetingAnnotations = externalTargetingAnnotations(schema, typeName);
    List<PropertyMetadata> properties = new ArrayList<>();
    for (CsdlProperty property : entityType.getProperties()) {
      List<CsdlAnnotation> externalAnnotations = externalTargetingAnnotations.get(property.getName());
      PropertyMetadata propertyMetadata = csdlToProperty(property, externalAnnotations);
      properties.add(propertyMetadata);
    }

    return new EntityType(entityType.getName(), properties);
  }

  private Map<String, List<CsdlAnnotation>> externalTargetingAnnotations(CsdlSchema schema, String entityTypeName) {
    List<CsdlAnnotations> annotations = schema.getAnnotationGroups();
    if (annotations == null || annotations.isEmpty()) {
      return Collections.emptyMap();
    }
    // Target starts either with namespace or its alias.
    String entityTypeNameFQN = String.format("%s.%s", schema.getNamespace(), entityTypeName);
    String entityTypeNameWithAlias = String.format("%s.%s", schema.getAlias(), entityTypeName);
    return annotations.stream()
      .filter(a -> a.getTarget().startsWith(entityTypeNameFQN) || a.getTarget().startsWith(entityTypeNameWithAlias))
      .collect(Collectors.toMap(a -> {
        String targetFQN = a.getTarget();
        return targetFQN.substring(targetFQN.lastIndexOf("/") + 1);
      }, CsdlAnnotations::getAnnotations));
  }

  private PropertyMetadata csdlToProperty(CsdlProperty property, List<CsdlAnnotation> externalAnnotations) {
    String type = property.getType();
    boolean nullable = property.isNullable();
    Integer precision = property.getPrecision();
    Integer scale = property.getScale();
    List<CsdlAnnotation> annotations = property.getAnnotations();

    if (annotations == null && externalAnnotations == null) {
      return new PropertyMetadata(property.getName(), type, nullable, precision, scale, null);
    }

    Stream<CsdlAnnotation> annotationStream = annotations != null && externalAnnotations == null ? annotations.stream()
      : annotations == null ? externalAnnotations.stream()
      : Stream.concat(annotations.stream(), externalAnnotations.stream());

    // include metadata annotations
    List<ODataAnnotation> oDataAnnotations = annotationStream
      .map(OData4Annotation::new)
      .collect(Collectors.toList());

    return new PropertyMetadata(property.getName(), type, nullable, precision, scale, oDataAnnotations);
  }
}
