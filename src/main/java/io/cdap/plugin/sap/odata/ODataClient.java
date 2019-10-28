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

import com.google.common.base.Strings;
import io.cdap.plugin.sap.odata.exception.ODataException;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * Abstract OData service client.
 */
public abstract class ODataClient {

  protected static final String METADATA = "$metadata";
  protected static final String SEPARATOR = "/";
  protected static final String QUERY_SEPARATOR = "?";

  protected final String rootUrl;
  protected final String username;
  protected final String password;

  /**
   * @param rootUrl  URL of the OData service. The URL must end with an external service name
   *                 (e.g., http://eccsvrname:8000/sap/opu/odata/sap/zgw100_dd02l_so_srv/).
   * @param username username for basic authentication.
   * @param password password for basic authentication.
   */
  public ODataClient(String rootUrl, String username, String password) {
    this.rootUrl = rootUrl;
    this.username = username;
    this.password = password;
  }

  /**
   * Query the specified entity set using OData query.
   *
   * @param entitySetName entity set name.
   * @param query         optional query such as "$top=2&$select=BuyerName&$filter=BuyerName eq 'TECUM'".
   * @return {@link ODataEntity} iterator for the specified entity set name.
   * @throws ODataException if the specified entity set cannot be read.
   */
  public abstract Iterator<ODataEntity> queryEntitySet(String entitySetName, @Nullable String query);

  /**
   * Get {@link EntityType} info for the specified entity set name.
   *
   * @param entitySetName entity set name.
   * @return {@link EntityType} for the specified entity set name.
   * @throws ODataException if the entity type does not exist or cannot be fetched.
   */
  public abstract EntityType getEntitySetType(String entitySetName);

  /**
   * Constructs a query URI according to the given entity set name and optional OData query.
   *
   * @param entitySetName entity set name.
   * @param query         optional OData query string.
   * @return query URI.
   */
  protected URI getQueryURI(String entitySetName, String query) {
    String entitySetUrl = rootUrl + SEPARATOR + entitySetName;
    String queryUrl = Strings.isNullOrEmpty(query) ? entitySetUrl : entitySetUrl + QUERY_SEPARATOR + query;
    try {
      URL url = new URL(queryUrl);
      return new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(),
                     url.getRef());
    } catch (MalformedURLException | URISyntaxException e) {
      throw new ODataException(String.format("Invalid URL: '%s'", queryUrl), e);
    }
  }

  /**
   * Constructs metadata URI.
   *
   * @return metadata URI.
   */
  protected URI getMetadataURI() {
    String metadataUrl = rootUrl + SEPARATOR + METADATA;
    try {
      URL url = new URL(metadataUrl);
      return new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(),
                     url.getRef());
    } catch (MalformedURLException | URISyntaxException e) {
      throw new ODataException(String.format("Invalid URL: '%s'", metadataUrl), e);
    }
  }

  /**
   * Indicates whether basic auth is required.
   *
   * @return {@code true} if basic auth is required, {@code false} otherwise.
   */
  protected boolean isAuthRequired() {
    return !Strings.isNullOrEmpty(username) || !Strings.isNullOrEmpty(password);
  }
}
