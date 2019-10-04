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

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.edm.provider.CsdlComplexType;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * OData complex property metadata.
 */
public class ComplexPropertyMetadata extends PropertyMetadata {

  private final CsdlSchema schema;
  private final CsdlComplexType complexType;
  private final CsdlComplexType baseType;

  public ComplexPropertyMetadata(String name, String edmTypeName, boolean isCollection, boolean isNullable,
                                 Integer precision, Integer scale, List<ODataAnnotation> annotations, CsdlSchema schema,
                                 CsdlComplexType complexType) {
    super(name, edmTypeName, isCollection, isNullable, precision, scale, annotations);
    this.schema = schema;
    this.complexType = complexType;
    FullQualifiedName baseTypeFQN = complexType.getBaseTypeFQN();
    this.baseType = baseTypeFQN != null ? schema.getComplexType(baseTypeFQN.getName()) : null;
  }

  public List<PropertyMetadata> getProperties() {
    Stream<CsdlProperty> propertyStream = baseType != null
      ? Stream.concat(baseType.getProperties().stream(), complexType.getProperties().stream())
      : complexType.getProperties().stream();

    return propertyStream.map(p -> PropertyMetadata.valueOf(schema, p, null)).collect(Collectors.toList());
  }

  @Override
  public boolean isComplex() {
    return true;
  }
}
