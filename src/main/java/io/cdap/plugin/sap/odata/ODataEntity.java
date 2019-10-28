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
import org.apache.olingo.client.api.domain.ClientProperty;
import org.apache.olingo.odata2.api.ep.entry.ODataEntry;

import java.util.HashMap;
import java.util.Map;
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

    return new ODataEntity(properties);
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
