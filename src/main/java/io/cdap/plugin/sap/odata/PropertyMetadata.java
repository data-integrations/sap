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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * OData property metadata.
 */
public class PropertyMetadata {

  private final String name;
  private final String edmTypeName;
  private final boolean nullable;
  private final Map<String, String> annotations;

  @Nullable
  private final Integer precision;

  @Nullable
  private final Integer scale;

  public PropertyMetadata(String name, String edmTypeName, boolean nullable, Integer precision, Integer scale,
                          Map<String, String> annotations) {
    this.name = name;
    this.edmTypeName = edmTypeName;
    this.nullable = nullable;
    this.precision = precision;
    this.scale = scale;
    this.annotations = annotations != null ? Collections.unmodifiableMap(new HashMap<>(annotations))
      : Collections.emptyMap();
  }

  public String getName() {
    return name;
  }

  public String getEdmTypeName() {
    return edmTypeName;
  }

  public boolean isNullable() {
    return nullable;
  }

  @Nullable
  public Integer getPrecision() {
    return precision;
  }

  @Nullable
  public Integer getScale() {
    return scale;
  }

  public Map<String, String> getAnnotations() {
    return annotations;
  }
}
