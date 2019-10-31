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

import org.apache.olingo.client.api.domain.ClientLink;

import javax.annotation.Nullable;

/**
 * OData Stream property. Single 'Edm.Stream' property annotated with both 'mediaReadLink' and 'mediaEditLink' will be
 * represented as two separate instances of Olingo {@link ClientLink}. This class is used to store data from these
 * instances in a single property.
 * See:
 * <a href="https://docs.oasis-open.org/odata/odata-json-format/v4.01/csprd05/odata-json-format-v4.01-csprd05.html">
 * "Stream PropertyMetadata" Section of the "OData JSON Format Version 4.01" document
 * </a>
 */
public class StreamProperty {

  @Nullable
  private final String mediaEtag;

  @Nullable
  private final String mediaContentType;

  @Nullable
  private final String mediaReadLink;

  @Nullable
  private final String mediaEditLink;

  public StreamProperty(@Nullable String mediaEtag, @Nullable String mediaContentType, @Nullable String mediaReadLink,
                        @Nullable String mediaEditLink) {
    this.mediaEtag = mediaEtag;
    this.mediaContentType = mediaContentType;
    this.mediaReadLink = mediaReadLink;
    this.mediaEditLink = mediaEditLink;
  }

  @Nullable
  public String getMediaEtag() {
    return mediaEtag;
  }

  @Nullable
  public String getMediaContentType() {
    return mediaContentType;
  }

  @Nullable
  public String getMediaReadLink() {
    return mediaReadLink;
  }

  @Nullable
  public String getMediaEditLink() {
    return mediaEditLink;
  }
}
