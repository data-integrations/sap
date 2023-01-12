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

/**
 * Provides handy methods to construct a {@link SapODataConfig} instance for testing.
 */
public final class SapODataConfigBuilder {

  private String referenceName;
  private String url;
  private String resourcePath;
  private String query;
  private String user;
  private String password;
  private String schema;
  private boolean includeMetadataAnnotations;

  private SapODataConfigBuilder() {
  }

  public static SapODataConfigBuilder builder() {
    return new SapODataConfigBuilder();
  }

  public static SapODataConfigBuilder builder(SapODataConfig original) {
    return builder()
      .setReferenceName(original.getReferenceName())
      .setUrl(original.getUrl())
      .setResourcePath(original.getResourcePath())
      .setQuery(original.getQuery())
      .setUser(original.getUser())
      .setPassword(original.getPassword())
      .setSchema(original.getSchema());
  }

  public SapODataConfigBuilder setReferenceName(String referenceName) {
    this.referenceName = referenceName;
    return this;
  }

  public SapODataConfigBuilder setUrl(String url) {
    this.url = url;
    return this;
  }

  public SapODataConfigBuilder setResourcePath(String resourcePath) {
    this.resourcePath = resourcePath;
    return this;
  }

  public SapODataConfigBuilder setQuery(String query) {
    this.query = query;
    return this;
  }

  public SapODataConfigBuilder setUser(String user) {
    this.user = user;
    return this;
  }

  public SapODataConfigBuilder setPassword(String password) {
    this.password = password;
    return this;
  }

  public SapODataConfigBuilder setSchema(String schema) {
    this.schema = schema;
    return this;
  }

  public SapODataConfigBuilder setIncludeMetadataAnnotations(boolean includeMetadataAnnotations) {
    this.includeMetadataAnnotations = includeMetadataAnnotations;
    return this;
  }

  public SapODataConfig build() {
    return new SapODataConfig(referenceName, url, resourcePath, query, user, password, schema,
                              includeMetadataAnnotations);
  }
}
