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

import io.cdap.plugin.sap.odata.ODataEntity;
import org.apache.olingo.odata2.api.edm.EdmLiteralKind;
import org.apache.olingo.odata2.api.edm.EdmSimpleTypeException;
import org.apache.olingo.odata2.core.edm.AbstractSimpleType;
import org.apache.olingo.odata2.core.edm.EdmBinary;
import org.apache.olingo.odata2.core.edm.EdmBoolean;
import org.apache.olingo.odata2.core.edm.EdmByte;
import org.apache.olingo.odata2.core.edm.EdmDateTime;
import org.apache.olingo.odata2.core.edm.EdmDateTimeOffset;
import org.apache.olingo.odata2.core.edm.EdmDecimal;
import org.apache.olingo.odata2.core.edm.EdmDouble;
import org.apache.olingo.odata2.core.edm.EdmGuid;
import org.apache.olingo.odata2.core.edm.EdmInt16;
import org.apache.olingo.odata2.core.edm.EdmInt32;
import org.apache.olingo.odata2.core.edm.EdmInt64;
import org.apache.olingo.odata2.core.edm.EdmSByte;
import org.apache.olingo.odata2.core.edm.EdmSingle;
import org.apache.olingo.odata2.core.edm.EdmString;
import org.apache.olingo.odata2.core.edm.EdmTime;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Provides handy methods to construct a {@link ODataEntity} instance for testing.
 */
public final class ODataEntityBuilder {

  private final Map<String, Object> properties;

  private ODataEntityBuilder() {
    this.properties = new HashMap<>();
  }

  public static ODataEntityBuilder builder() {
    return new ODataEntityBuilder();
  }

  protected ODataEntityBuilder set(String name, Object value, AbstractSimpleType type) {
    try {
      // Trigger validation
      type.valueToString(value, EdmLiteralKind.DEFAULT, null);
    } catch (EdmSimpleTypeException e) {
      throw new IllegalArgumentException(e);
    }
    this.properties.put(name, value);
    return this;
  }

  public ODataEntityBuilder setInt16(String name, short value) {
    return set(name, value, EdmInt16.getInstance());
  }

  public ODataEntityBuilder setInt32(String name, int value) {
    return set(name, value, EdmInt32.getInstance());
  }

  public ODataEntityBuilder setInt64(String name, long value) {
    return set(name, value, EdmInt64.getInstance());
  }

  public ODataEntityBuilder setBinary(String name, byte[] value) {
    return set(name, value, EdmBinary.getInstance());
  }

  public ODataEntityBuilder setBoolean(String name, boolean value) {
    return set(name, value, EdmBoolean.getInstance());
  }

  public ODataEntityBuilder setByte(String name, short value) {
    return set(name, value, EdmByte.getInstance());
  }

  public ODataEntityBuilder setDateTime(String name, Calendar value) {
    return set(name, value, EdmDateTime.getInstance());
  }

  public ODataEntityBuilder setDecimal(String name, BigDecimal value) {
    return set(name, value, EdmDecimal.getInstance());
  }

  public ODataEntityBuilder setDouble(String name, double value) {
    return set(name, value, EdmDouble.getInstance());
  }

  public ODataEntityBuilder setSingle(String name, float value) {
    return set(name, value, EdmSingle.getInstance());
  }

  public ODataEntityBuilder setGuid(String name, UUID value) {
    return set(name, value, EdmGuid.getInstance());
  }

  public ODataEntityBuilder setSByte(String name, byte value) {
    return set(name, value, EdmSByte.getInstance());
  }

  public ODataEntityBuilder setString(String name, String value) {
    return set(name, value, EdmString.getInstance());
  }

  public ODataEntityBuilder setTime(String name, Calendar value) {
    return set(name, value, EdmTime.getInstance());
  }

  public ODataEntityBuilder setDateTimeOffset(String name, String value) throws EdmSimpleTypeException {
    EdmDateTimeOffset edmDateTimeOffset = EdmDateTimeOffset.getInstance();
    Calendar calendar = edmDateTimeOffset.valueOfString(value, EdmLiteralKind.DEFAULT, null, Calendar.class);
    return set(name, calendar, edmDateTimeOffset);
  }

  public ODataEntity build() {
    return new ODataEntity(properties);
  }
}
