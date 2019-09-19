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

import org.apache.olingo.commons.api.edm.annotation.EdmExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlConstantExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlDynamicExpression;
import org.apache.olingo.commons.api.edm.provider.annotation.CsdlExpression;

/**
 * OData annotation metadata.
 */
public class ODataUtil {

  private ODataUtil() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  public static EdmExpression.EdmExpressionType typeOf(CsdlExpression expression) {
    return expression.isConstant() ? typeOf(expression.asConstant()) : typeOf(expression.asDynamic());
  }

  public static EdmExpression.EdmExpressionType typeOf(CsdlConstantExpression constant) {
    // type names match
    return EdmExpression.EdmExpressionType.valueOf(constant.getType().name());
  }

  public static EdmExpression.EdmExpressionType typeOf(CsdlDynamicExpression dynamic) {
    if (dynamic.isPath()) {
      return EdmExpression.EdmExpressionType.Path;
    }
    if (dynamic.isAnnotationPath()) {
      return EdmExpression.EdmExpressionType.AnnotationPath;
    }
    if (dynamic.isLabeledElementReference()) {
      return EdmExpression.EdmExpressionType.LabeledElementReference;
    }
    if (dynamic.isNavigationPropertyPath()) {
      return EdmExpression.EdmExpressionType.NavigationPropertyPath;
    }
    if (dynamic.isPropertyPath()) {
      return EdmExpression.EdmExpressionType.PropertyPath;
    }
    if (dynamic.isNull()) {
      return EdmExpression.EdmExpressionType.Null;
    }
    if (dynamic.isApply()) {
      return EdmExpression.EdmExpressionType.Apply;
    }
    if (dynamic.isCast()) {
      return EdmExpression.EdmExpressionType.Cast;
    }
    if (dynamic.isCollection()) {
      return EdmExpression.EdmExpressionType.Collection;
    }
    if (dynamic.isIf()) {
      return EdmExpression.EdmExpressionType.If;
    }
    if (dynamic.isIsOf()) {
      return EdmExpression.EdmExpressionType.IsOf;
    }
    if (dynamic.isLabeledElement()) {
      return EdmExpression.EdmExpressionType.LabeledElement;
    }
    if (dynamic.isRecord()) {
      return EdmExpression.EdmExpressionType.Record;
    }
    if (dynamic.isUrlRef()) {
      return EdmExpression.EdmExpressionType.UrlRef;
    }

    // Expression can only be a logical at this point.
    // Type names match.
    String csdlTypeName = dynamic.asLogicalOrComparison().getType().name();
    return EdmExpression.EdmExpressionType.valueOf(csdlTypeName);
  }
}
