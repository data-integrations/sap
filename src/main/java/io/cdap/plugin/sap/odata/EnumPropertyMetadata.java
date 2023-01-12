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

import org.apache.olingo.commons.api.edm.provider.CsdlEnumMember;
import org.apache.olingo.commons.api.edm.provider.CsdlEnumType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * OData enum property metadata.
 */
public class EnumPropertyMetadata extends PropertyMetadata {

  private final CsdlEnumType enumType;

  public EnumPropertyMetadata(String name, String edmTypeName, boolean isCollection, boolean isNullable,
                              Integer precision, Integer scale, List<ODataAnnotation> annotations,
                              CsdlEnumType enumType) {
    super(name, edmTypeName, isCollection, isNullable, precision, scale, annotations);
    this.enumType = enumType;
  }

  public List<String> getValues() {
    return enumType.getMembers().stream()
      .map(CsdlEnumMember::getName)
      .collect(Collectors.toList());
  }

  @Override
  public boolean isEnum() {
    return true;
  }
}
