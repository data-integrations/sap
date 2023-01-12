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

import io.cdap.plugin.sap.odata.ODataAnnotation;

/**
 * OData annotation metadata.
 */
public class OData2Annotation extends ODataAnnotation {

  private final String name;
  private final String value;

  public OData2Annotation(String name, String value) {
    this.name = name;
    this.value = value;
  }

  @Override
  public String getName() {
    return this.name;
  }

  public String getValue() {
    return this.value;
  }
}
