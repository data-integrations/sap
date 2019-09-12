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

import com.google.common.base.Strings;
import io.cdap.plugin.sap.exception.ODataException;
import org.apache.olingo.odata2.api.edm.Edm;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmEntityType;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderException;
import org.apache.olingo.odata2.api.ep.EntityProviderReadProperties;
import org.apache.olingo.odata2.api.ep.feed.ODataFeed;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import javax.annotation.Nullable;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;

/**
 * Provides handy methods to consume OData v2 services using Apache Olingo v2 as Client Library.
 */
public class OData2Client {

  private static final String METADATA = "$metadata";
  private static final String SEPARATOR = "/";
  private static final String QUERY_SEPARATOR = "?";

  private final String rootUrl;
  private final String username;
  private final String password;

  /**
   * @param rootUrl  URL of the OData service. The URL must end with an external service name
   *                 (e.g., http://eccsvrname:8000/sap/opu/odata/sap/zgw100_dd02l_so_srv/).
   * @param username username for basic authentication.
   * @param password password for basic authentication.
   */
  public OData2Client(String rootUrl, @Nullable String username, @Nullable String password) {
    this.rootUrl = rootUrl;
    this.username = username;
    this.password = password;
  }

  /**
   * Query {@link ODataFeed} using the specified entity set name and OData query.
   *
   * @param entitySetName entity set name.
   * @param query         query such as "$top=2&$select=BuyerName&$filter=BuyerName eq 'TECUM'".
   * @return {@link ODataFeed} for the specified entity set name.
   * @throws ODataException if the specified entity set cannot be read.
   */
  public ODataFeed queryEntitySet(String entitySetName, @Nullable String query) {
    Edm metadata = getMetadata();
    String entitySetUrl = rootUrl + SEPARATOR + entitySetName;
    String queryUrl = Strings.isNullOrEmpty(query) ? entitySetUrl : entitySetUrl + QUERY_SEPARATOR + query;
    HttpURLConnection connection = connect(queryUrl, username, password);
    try {
      EdmEntitySet entitySet = metadata.getDefaultEntityContainer().getEntitySet(entitySetName);
      InputStream content = (InputStream) connection.getContent();
      return EntityProvider.readFeed(MediaType.APPLICATION_XML, entitySet, content,
                                     EntityProviderReadProperties.init().build());
    } catch (IOException | EdmException | EntityProviderException e) {
      throw new ODataException(String.format("Unable to read '%s' entity set.", entitySetName), e);
    } finally {
      // will close the content InputStream
      connection.disconnect();
    }
  }

  /**
   * Get OData service metadata.
   *
   * @return OData service metadata.
   * @throws ODataException if the metadata cannot be fetched.
   */
  public Edm getMetadata() {
    String metadataUrl = rootUrl + SEPARATOR + METADATA;
    HttpURLConnection connection = connect(metadataUrl, username, password);
    try {
      InputStream content = connection.getInputStream();
      return EntityProvider.readMetadata(content, false);
    } catch (IOException | EntityProviderException e) {
      throw new ODataException("Unable to get metadata: " + e.getMessage(), e);
    } finally {
      // will close the content InputStream
      connection.disconnect();
    }
  }

  /**
   * Get {@link EdmEntityType} for the specified entity set name.
   *
   * @param entitySetName entity set name.
   * @return {@link EdmEntityType} for the specified entity set name.
   * @throws ODataException if the {@link EdmEntityType} does not exists or cannot be fetched.
   */
  public EdmEntityType getEntitySetType(String entitySetName) {
    Edm edm = getMetadata();
    try {
      List<EdmEntitySet> entitySets = edm.getDefaultEntityContainer().getEntitySets();
      for (EdmEntitySet entitySet : entitySets) {
        if (entitySetName.equals(entitySet.getName())) {
          return entitySet.getEntityType();
        }
      }
    } catch (EdmException e) {
      throw new ODataException("Unable to get entity set type: " + e.getMessage(), e);
    }

    throw new ODataException(String.format("Unable to find entity type for '%s' entity set.", entitySetName));
  }

  private HttpURLConnection connect(String url, String username, String password) {
    String encodedUrl = urlEncode(url);
    try {
      HttpURLConnection connection = (HttpURLConnection) new URL(encodedUrl).openConnection();
      connection.setRequestMethod(HttpMethod.GET);
      connection.setRequestProperty(HttpHeaders.ACCEPT, MediaType.APPLICATION_XML);
      if (!Strings.isNullOrEmpty(username) || !Strings.isNullOrEmpty(password)) {
        byte[] credentials = (username + ":" + password).getBytes(StandardCharsets.UTF_8);
        String encoded = Base64.getEncoder().encodeToString(credentials);
        connection.setRequestProperty(HttpHeaders.AUTHORIZATION, "Basic " + encoded);
      }
      connection.connect();
      return connection;
    } catch (IOException e) {
      throw new ODataException(String.format("Unable to connect to '%s': %s", e.getMessage(), encodedUrl), e);
    }
  }

  /**
   * Encode the specified string representation of URL.
   * For example, query "$top=2&$skip=2&$select=BuyerName&$filter=BuyerName eq 'TECUM'" will be encoded as to
   * "$top=2&$skip=2&$select=BuyerName&$filter=BuyerName%20eq%20%27TECUM%27".
   * Using {@link URLEncoder#encode} is not appropriate since it will encode allowed characters, such as '$' too.
   */
  private String urlEncode(String original) {
    try {
      URL url = new URL(original);
      URI uri = new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(),
                        url.getQuery(), url.getRef());
      return uri.toASCIIString();
    } catch (URISyntaxException | MalformedURLException e) {
      throw new ODataException(String.format("Unable to encode URL: '%s'", original), e);
    }
  }
}
