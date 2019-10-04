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

package io.cdap.plugin.sap.odata.odata2;

import io.cdap.plugin.sap.odata.EntityType;
import io.cdap.plugin.sap.odata.ODataAnnotation;
import io.cdap.plugin.sap.odata.ODataClient;
import io.cdap.plugin.sap.odata.ODataEntity;
import io.cdap.plugin.sap.odata.PropertyMetadata;
import io.cdap.plugin.sap.odata.exception.ODataException;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.edm.EdmAnnotationAttribute;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmEntityType;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderException;
import org.apache.olingo.odata2.api.ep.EntityProviderReadProperties;
import org.apache.olingo.odata2.api.ep.feed.ODataFeed;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

/**
 * Provides handy methods to consume OData v2 services using Apache Olingo v2 as Client Library.
 */
public class OData2Client extends ODataClient {

  /**
   * Default {@link EntityProvider} settings to read an entity.
   */
  private static final EntityProviderReadProperties READ_PROPERTIES = EntityProviderReadProperties.init().build();

  /**
   * Cache metadata to avoid repetitive API calls.
   */
  private Edm metadata;

  /**
   * @param rootUrl  URL of the OData service. The URL must end with an external service name
   *                 (e.g., http://eccsvrname:8000/sap/opu/odata/sap/zgw100_dd02l_so_srv/).
   * @param username username for basic authentication.
   * @param password password for basic authentication.
   */
  public OData2Client(String rootUrl, String username, String password) {
    super(rootUrl, username, password);
  }

  /**
   * Get OData service metadata.
   *
   * @return OData service metadata.
   * @throws ODataException if the metadata cannot be fetched.
   */
  public Edm getMetadata() {
    if (metadata == null) {
      initMetadata();
    }
    return metadata;
  }

  @Override
  public Iterator<ODataEntity> queryEntitySet(String entitySetName, @Nullable String query) {
    Edm metadata = getMetadata();
    URI queryURI = getQueryURI(entitySetName, query);
    HttpURLConnection connection = connect(queryURI.toASCIIString(), MediaType.APPLICATION_ATOM_XML);
    try (InputStream content = (InputStream) connection.getContent()) {
      EdmEntitySet entitySet = metadata.getDefaultEntityContainer().getEntitySet(entitySetName);
      ODataFeed feed = EntityProvider.readFeed(connection.getContentType(), entitySet, content, READ_PROPERTIES);

      return new OData2EntityIterator(feed.getEntries().iterator());
    } catch (IOException | EdmException | EntityProviderException e) {
      throw new ODataException(String.format("Unable to read '%s' entity set.", entitySetName), e);
    } finally {
      // will close the content InputStream
      connection.disconnect();
    }
  }

  @Override
  public EntityType getEntitySetType(String entitySetName) {
    try {
      EdmEntitySet entitySet = getMetadata().getDefaultEntityContainer().getEntitySet(entitySetName);
      EdmEntityType edmEntityType = entitySet.getEntityType();
      List<PropertyMetadata> properties = new ArrayList<>();
      for (String propertyName : edmEntityType.getPropertyNames()) {
        EdmProperty property = (EdmProperty) edmEntityType.getProperty(propertyName);
        properties.add(edmToProperty(property));
      }

      return new EntityType(edmEntityType.getName(), properties);
    } catch (EdmException e) {
      throw new ODataException("Unable to get entity set type: " + e.getMessage(), e);
    }
  }

  private PropertyMetadata edmToProperty(EdmProperty property) throws EdmException {
    String type = property.getType().getName();
    boolean nullable = property.getFacets().isNullable();
    Integer precision = property.getFacets().getPrecision();
    Integer scale = property.getFacets().getScale();
    List<EdmAnnotationAttribute> annotationAttributes = property.getAnnotations().getAnnotationAttributes();
    List<ODataAnnotation> annotations = annotationAttributes == null ? null : annotationAttributes.stream()
      .map(a -> new OData2Annotation(a.getName(), a.getText()))
      .collect(Collectors.toList());

    // todo is collection?
    return new PropertyMetadata(property.getName(), type, false, nullable, precision, scale, annotations);
  }

  private void initMetadata() {
    HttpURLConnection connection = connect(getMetadataURI().toASCIIString(), MediaType.APPLICATION_XML);
    try (InputStream content = connection.getInputStream()) {
      metadata = EntityProvider.readMetadata(content, false);
    } catch (IOException | EntityProviderException e) {
      throw new ODataException("Unable to get metadata: " + e.getMessage(), e);
    } finally {
      // will close the content InputStream
      connection.disconnect();
    }
  }

  private HttpURLConnection connect(String url, String medialType) {
    try {
      HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
      connection.setRequestMethod(HttpMethod.GET);
      connection.setRequestProperty(HttpHeaders.ACCEPT, medialType);
      if (isAuthRequired()) {
        connection.setRequestProperty(HttpHeaders.AUTHORIZATION, getAuthHeaderValue());
      }
      connection.connect();
      return connection;
    } catch (IOException e) {
      throw new ODataException(String.format("Unable to connect to '%s': %s", url, e.getMessage()), e);
    }
  }

  private String getAuthHeaderValue() {
    byte[] credentials = (username + ":" + password).getBytes(StandardCharsets.UTF_8);
    String encoded = Base64.getEncoder().encodeToString(credentials);
    return "Basic " + encoded;
  }
}
