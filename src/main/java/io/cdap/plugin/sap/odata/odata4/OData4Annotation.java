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

package io.cdap.plugin.sap.odata.odata4;

import com.google.common.base.Strings;
import io.cdap.plugin.sap.odata.ODataAnnotation;
import org.apache.olingo.commons.api.edm.EdmAnnotation;
import org.apache.olingo.commons.api.edm.annotation.EdmExpression;

/**
 * OData annotation metadata.
 */
public class OData4Annotation extends ODataAnnotation {

  private final EdmAnnotation annotation;

  public OData4Annotation(EdmAnnotation annotation) {
    this.annotation = annotation;
  }

  public String getTerm() {
    return annotation.getTerm().getFullQualifiedName().getFullQualifiedNameAsString();
  }

  public String getQualifier() {
    return annotation.getQualifier();
  }
  public EdmExpression getExpression() {
    return annotation.getExpression();
  }

  /**
   * Replaces any character that are not one of [A-Z][a-z][0-9] or _ with an underscore (_).
   * @return annotation name
   */
  @Override
  public String getName() {
    String qualifier = annotation.getQualifier();
    String termName = annotation.getTerm().getName();
    String annotationName = Strings.isNullOrEmpty(qualifier) ?  termName : qualifier + "_" + termName;
    return annotationName.toLowerCase().replaceAll("[^A-Za-z0-9]", "_");
  }
}
