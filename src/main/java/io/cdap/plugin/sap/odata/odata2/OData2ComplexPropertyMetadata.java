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

import io.cdap.plugin.sap.odata.ComplexPropertyMetadata;
import io.cdap.plugin.sap.odata.ODataAnnotation;
import io.cdap.plugin.sap.odata.PropertyMetadata;
import io.cdap.plugin.sap.odata.exception.ODataException;
import org.apache.olingo.odata2.api.edm.EdmComplexType;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * OData V2 complex property metadata.
 */
public class OData2ComplexPropertyMetadata extends PropertyMetadata implements ComplexPropertyMetadata {

  private final EdmComplexType complexType;

  public OData2ComplexPropertyMetadata(String name, String edmTypeName, boolean isCollection, boolean isNullable,
                                       Integer precision, Integer scale, List<ODataAnnotation> annotations,
                                       EdmComplexType complexType) {
    super(name, edmTypeName, isCollection, isNullable, precision, scale, annotations);
    this.complexType = complexType;
  }

  public List<PropertyMetadata> getProperties() {
    try {
      List<PropertyMetadata> properties = new ArrayList<>();
      for (String propertyName : complexType.getPropertyNames()) {
        EdmProperty property = (EdmProperty) complexType.getProperty(propertyName);
        PropertyMetadata propertyMetadata = PropertyMetadata.valueOf(property);
        properties.add(propertyMetadata);
      }
      return properties;
    } catch (EdmException e) {
      throw new ODataException(String.format("Unable to get properties of '%s' complex type: '%s'", getName(),
                                             e.getMessage()), e);
    }
  }

  @Override
  public boolean isComplex() {
    return true;
  }
}
