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
 * SAP OData constants.
 */
public class SapODataConstants {

  private SapODataConstants() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * SAP plugin name.
   */
  public static final String PLUGIN_NAME = "SapOData";

  /**
   * Configuration property name used to specify URL of the SAP OData service.
   */
  public static final String ODATA_SERVICE_URL = "url";

  /**
   * Configuration property name used to specify path of the SAP OData entity.
   */
  public static final String RESOURCE_PATH = "resourcePath";

  /**
   * Configuration property name used to specify OData query options to filter the data.
   */
  public static final String QUERY = "query";

  /**
   * Configuration property name used to specify username for basic authentication.
   */
  public static final String USERNAME = "username";

  /**
   * Configuration property name used to specify password for basic authentication.
   */
  public static final String PASSWORD = "password";

  /**
   * Configuration property name used to specify the schema of the entries.
   */
  public static final String SCHEMA = "schema";
}
