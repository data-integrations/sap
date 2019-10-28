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

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * OData protocol version.
 */
public enum ODataVersion {

  V2("1.0"),
  V4("4.0");

  private static final Map<String, ODataVersion> byEdmVersion = Arrays.stream(values())
    .collect(Collectors.toMap(ODataVersion::getEdmVersion, Function.identity()));

  private final String edmVersion;

  /**
   * OData V2 uses Edm version 1.0 and OData V4 uses Edm version 4.0
   */
  ODataVersion(String edmVersion) {
    this.edmVersion = edmVersion;
  }

  @Nullable
  public static ODataVersion fromEdmVersion(String edmVersion) {
    return byEdmVersion.get(edmVersion);
  }

  public String getEdmVersion() {
    return edmVersion;
  }
}
