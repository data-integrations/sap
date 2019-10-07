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

import io.cdap.plugin.sap.odata.odata2.OData2Annotation;
import io.cdap.plugin.sap.odata.odata2.OData2ComplexPropertyMetadata;
import io.cdap.plugin.sap.odata.odata4.OData4Annotation;
import io.cdap.plugin.sap.odata.odata4.OData4ComplexPropertyMetadata;
import org.apache.olingo.commons.api.edm.provider.CsdlAnnotation;
import org.apache.olingo.commons.api.edm.provider.CsdlComplexType;
import org.apache.olingo.commons.api.edm.provider.CsdlEnumType;
import org.apache.olingo.commons.api.edm.provider.CsdlProperty;
import org.apache.olingo.commons.api.edm.provider.CsdlSchema;
import org.apache.olingo.commons.api.edm.provider.CsdlTypeDefinition;
import org.apache.olingo.odata2.api.edm.EdmAnnotationAttribute;
import org.apache.olingo.odata2.api.edm.EdmComplexType;
import org.apache.olingo.odata2.api.edm.EdmException;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.edm.EdmType;
import org.apache.olingo.odata2.api.edm.EdmTypeKind;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * OData property metadata.
 */
public class PropertyMetadata {

  private final String name;

  private final String type;
  private final boolean isNullable;
  private final boolean isCollection;
  private final List<ODataAnnotation> annotations;

  @Nullable
  private final Integer precision;

  @Nullable
  private final Integer scale;

  public PropertyMetadata(String name, String type, boolean isCollection, boolean isNullable, Integer precision,
                          Integer scale, List<ODataAnnotation> annotations) {
    this.name = name;
    this.type = type;
    this.isCollection = isCollection;
    this.isNullable = isNullable;
    this.precision = precision;
    this.scale = scale;
    this.annotations = annotations;
  }

  public static PropertyMetadata valueOf(EdmProperty property) throws EdmException {
    boolean nullable = property.getFacets().isNullable();
    Integer precision = property.getFacets().getPrecision();
    Integer scale = property.getFacets().getScale();
    List<EdmAnnotationAttribute> annotationAttributes = property.getAnnotations().getAnnotationAttributes();
    List<ODataAnnotation> annotations = annotationAttributes == null ? null : annotationAttributes.stream()
      .map(a -> new OData2Annotation(a.getName(), a.getText()))
      .collect(Collectors.toList());

    EdmType type = property.getType();
    if (type.getKind() == EdmTypeKind.COMPLEX) {
      EdmComplexType complexType = ((EdmComplexType) property.getType());
      return new OData2ComplexPropertyMetadata(property.getName(), type.getName(), false, nullable,
                                               precision, scale, annotations, complexType);
    }

    return new PropertyMetadata(property.getName(), type.getName(), false, nullable, precision, scale, annotations);
  }

  public static PropertyMetadata valueOf(CsdlSchema schema, CsdlProperty property,
                                         @Nullable List<ODataAnnotation> externalAnnotations) {
    String name = property.getName();
    String type = property.getType();
    boolean isNullable = property.isNullable();
    boolean isCollection = property.isCollection();
    Integer precision = property.getPrecision();
    Integer scale = property.getScale();
    List<ODataAnnotation> annotations = getPropertyAnnotations(property);
    if (externalAnnotations != null) {
      annotations.addAll(externalAnnotations);
    }

    String simplePropertyName = property.getTypeAsFQNObject().getName();
    CsdlComplexType complexType = schema.getComplexType(simplePropertyName);
    if (complexType != null) {
      return new OData4ComplexPropertyMetadata(name, type, isCollection, isNullable, precision, scale, annotations,
                                               schema, complexType);
    }

    CsdlTypeDefinition typeDefinition = schema.getTypeDefinition(simplePropertyName);
    if (typeDefinition != null) {
      // use underlying type as property edm type
      return new PropertyMetadata(name, typeDefinition.getUnderlyingType(), isCollection, isNullable, precision, scale,
                                  annotations);
    }

    CsdlEnumType enumType = schema.getEnumType(simplePropertyName);
    if (enumType != null) {
      return new EnumPropertyMetadata(name, type, isCollection, isNullable, precision, scale, annotations, enumType);
    }

    return new PropertyMetadata(name, type, isCollection, isNullable, precision, scale, annotations);
  }

  private static List<ODataAnnotation> getPropertyAnnotations(CsdlProperty property) {
    List<CsdlAnnotation> annotations = property.getAnnotations();
    if (annotations == null) {
      return Collections.emptyList();
    }

    return annotations.stream()
      .map(OData4Annotation::new)
      .collect(Collectors.toList());
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public boolean isNullable() {
    return isNullable;
  }

  public boolean isCollection() {
    return isCollection;
  }

  @Nullable
  public Integer getPrecision() {
    return precision;
  }

  @Nullable
  public Integer getScale() {
    return scale;
  }

  public List<ODataAnnotation> getAnnotations() {
    return annotations;
  }

  public boolean isComplex() {
    return false;
  }

  public boolean isEnum() {
    return false;
  }
}
