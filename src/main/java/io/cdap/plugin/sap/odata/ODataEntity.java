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

import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientLink;
import org.apache.olingo.client.api.domain.ClientLinkType;
import org.apache.olingo.client.api.domain.ClientProperty;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * OData entity data. Common model for both Olingo V2 and V4 clients.
 */
public class ODataEntity {

  private final Map<String, Object> properties;

  public ODataEntity() {
    this.properties = new HashMap<>();
  }

  public ODataEntity(Map<String, Object> properties) {
    this.properties = properties;
  }

  public static ODataEntity valueOf(ODataEntry oDataEntry) {
    return new ODataEntity(oDataEntry.getProperties());
  }

  public static ODataEntity valueOf(ClientEntity clientEntity) {
    Map<String, Object> properties = clientEntity.getProperties().stream()
      .collect(HashMap::new, (m, v) -> m.put(v.getName(), getClientPropertyValue(v)), HashMap::putAll);
    // OData4 'Edm.Stream' properties can be accessed via ClientEntity#getMediaEditLinks
    if (clientEntity.getMediaEditLinks() != null && !clientEntity.getMediaEditLinks().isEmpty()) {
      Map<String, StreamProperty> streamProperties = extractStreamProperties(clientEntity);
      properties.putAll(streamProperties);
    }

    return new ODataEntity(properties);
  }

  /**
   * Single 'Edm.Stream' property annotated with both 'mediaReadLink' and 'mediaEditLink' will be represented as two
   * separate instances of Olingo {@link ClientLink}. This method maps such links to single {@link StreamProperty}
   * instance and returns map of stream properties with their names as keys.
   * See:
   * <a href="https://docs.oasis-open.org/odata/odata-json-format/v4.01/csprd05/odata-json-format-v4.01-csprd05.html">
   * "Stream PropertyMetadata" Section of the "OData JSON Format Version 4.01" document
   * </a>
   */
  static Map<String, StreamProperty> extractStreamProperties(ClientEntity clientEntity) {
    return clientEntity.getMediaEditLinks().stream()
      .filter(link -> link.getType() == ClientLinkType.MEDIA_READ || link.getType() == ClientLinkType.MEDIA_EDIT)
      .collect(Collectors.toMap(
        ClientLink::getName,
        link -> link.getType() == ClientLinkType.MEDIA_READ
          ? new StreamProperty(link.getMediaETag(), link.getType().toString(), link.getLink().toASCIIString(), null)
          : new StreamProperty(link.getMediaETag(), link.getType().toString(), null, link.getLink().toASCIIString()),
        (first, second) -> {
          String readLink = first.getMediaReadLink() == null ? second.getMediaReadLink() : first.getMediaReadLink();
          String editLink = first.getMediaEditLink() == null ? second.getMediaEditLink() : first.getMediaEditLink();
          return new StreamProperty(first.getMediaEtag(), first.getMediaContentType(), readLink, editLink);
        }
      ));
  }

  @Nullable
  static Object getClientPropertyValue(ClientProperty property) {
    if (property.hasPrimitiveValue()) {
      return property.getPrimitiveValue().toValue();
    }
    if (property.hasCollectionValue()) {
      return property.getCollectionValue().asJavaCollection();
    }
    if (property.hasNullValue()) {
      return null;
    }
    if (property.hasEnumValue()) {
      return property.getEnumValue().getValue();
    }
    if (property.hasComplexValue()) {
      throw new IllegalArgumentException("Complex types are not supported");
    }

    throw new IllegalArgumentException(String.format("Property '%s' has unsupported value: '%s'.",
                                                     property.getName(), property.getValue()));
  }

  public Map<String, Object> getProperties() {
    return properties;
  }
}
