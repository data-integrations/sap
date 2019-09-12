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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.Constants;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests of {@link SapODataConfig} methods.
 */
public class SapODataConfigTest {

  private static final String MOCK_STAGE_NAME = "mockstage";
  private static final String STAGE = "stage";

  private static final Schema VALID_SCHEMA =
    Schema.recordOf("schema",
                    Schema.Field.of("int_field", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("long_field", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                    Schema.Field.of("float_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("double_field", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
                    Schema.Field.of("string_field", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("boolean_field", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
                    Schema.Field.of("bytes_field", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                    Schema.Field.of("decimal_field", Schema.nullableOf(Schema.decimalOf(10, 4))),
                    Schema.Field.of("time_micros_field", Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MICROS))),
                    Schema.Field.of("time_millis_field", Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MILLIS))),
                    Schema.Field.of("ts_micros", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
                    Schema.Field.of("ts_millis", Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS))));

  private static final SapODataConfig VALID_CONFIG = SapODataConfigBuilder.builder()
    .setReferenceName("SapODataSource")
    .setUrl("http://vhcalnplci.dummy.nodomain:8000/sap/opu/odata/SAP/ZGW100_XX_S2_SRV/")
    .setResourcePath("SalesOrderCollection")
    .setQuery("$top=2&$skip=2&$select=BuyerName&$filter=BuyerName eq %27TECUM%27")
    .setUser("admin")
    .setPassword("password")
    .setSchema(VALID_SCHEMA.toString())
    .build();

  @Test
  public void testValidateValid() {
    try {
      VALID_CONFIG.validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      // MockFailureCollector throws an exception even if there are no validation errors
      Assert.assertTrue(e.getFailures().isEmpty());
    }
  }

  @Test
  public void testGetParsedSchema() {
    Assert.assertEquals(VALID_SCHEMA, VALID_CONFIG.getParsedSchema());
  }

  @Test
  public void testValidateReferenceNameNull() {
    try {
      SapODataConfigBuilder.builder(VALID_CONFIG)
        .setReferenceName(null)
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Reference name must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(Constants.Reference.REFERENCE_NAME, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateReferenceNameEmpty() {
    try {
      SapODataConfigBuilder.builder(VALID_CONFIG)
        .setReferenceName("")
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Reference name must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(Constants.Reference.REFERENCE_NAME, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateReferenceNameInvalid() {
    try {
      SapODataConfigBuilder.builder(VALID_CONFIG)
        .setReferenceName("**********")
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Invalid reference name", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(Constants.Reference.REFERENCE_NAME, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateUrlNull() {
    try {
      SapODataConfigBuilder.builder(VALID_CONFIG)
        .setUrl(null)
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("OData Service URL must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(SapODataConstants.ODATA_SERVICE_URL, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateUrlEmpty() {
    try {
      SapODataConfigBuilder.builder(VALID_CONFIG)
        .setUrl("")
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("OData Service URL must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(SapODataConstants.ODATA_SERVICE_URL, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateResourcePathNull() {
    try {
      SapODataConfigBuilder.builder(VALID_CONFIG)
        .setResourcePath(null)
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Resource path must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(SapODataConstants.RESOURCE_PATH, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateResourcePathEmpty() {
    try {
      SapODataConfigBuilder.builder(VALID_CONFIG)
        .setResourcePath("")
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      ValidationFailure validationFailure = e.getFailures().get(0);
      Assert.assertEquals("Resource path must be specified", validationFailure.getMessage());
      Assert.assertEquals(1, validationFailure.getCauses().size());
      ValidationFailure.Cause cause = validationFailure.getCauses().get(0);
      Assert.assertEquals(SapODataConstants.RESOURCE_PATH, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
      Assert.assertEquals(MOCK_STAGE_NAME, cause.getAttribute(STAGE));
    }
  }

  @Test
  public void testValidateNullQuery() {
    try {
      SapODataConfigBuilder.builder(VALID_CONFIG)
        .setQuery(null)
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      // MockFailureCollector throws an exception even if there are no validation errors
      Assert.assertTrue(e.getFailures().isEmpty());
    }
  }

  @Test
  public void testValidateEmptyQuery() {
    try {
      SapODataConfigBuilder.builder(VALID_CONFIG)
        .setQuery("")
        .build()
        .validate(new MockFailureCollector(MOCK_STAGE_NAME));
    } catch (ValidationException e) {
      // MockFailureCollector throws an exception even if there are no validation errors
      Assert.assertTrue(e.getFailures().isEmpty());
    }
  }

  @Test
  public void testSelectProperties() {
    List<String> selectProperties = SapODataConfigBuilder.builder(VALID_CONFIG)
      .setQuery("$select=BuyerName")
      .build()
      .getSelectProperties();

    Assert.assertEquals(Collections.singletonList("BuyerName"), selectProperties);
  }

  @Test
  public void testSelectPropertiesMultiple() {
    List<String> selectProperties = SapODataConfigBuilder.builder(VALID_CONFIG)
      .setQuery("$select=Buyer Name,First Name")
      .build()
      .getSelectProperties();

    Assert.assertEquals(Arrays.asList("Buyer Name", "First Name"), selectProperties);
  }

  @Test
  public void testSelectPropertiesAtStart() {
    List<String> selectProperties = SapODataConfigBuilder.builder(VALID_CONFIG)
      .setQuery("$select=BuyerName&$filter=BuyerName eq %27TECUM%27")
      .build()
      .getSelectProperties();

    Assert.assertEquals(Collections.singletonList("BuyerName"), selectProperties);
  }

  @Test
  public void testSelectPropertiesAtEnd() {
    List<String> selectProperties = SapODataConfigBuilder.builder(VALID_CONFIG)
      .setQuery("$top=2&$skip=2&$select=BuyerName")
      .build()
      .getSelectProperties();

    Assert.assertEquals(Collections.singletonList("BuyerName"), selectProperties);
  }

  @Test
  public void testSelectPropertiesAtMiddle() {
    List<String> selectProperties = SapODataConfigBuilder.builder(VALID_CONFIG)
      .setQuery("$top=2&$skip=2&$select=BuyerName&$filter=BuyerName eq %27TECUM%27")
      .build()
      .getSelectProperties();

    Assert.assertEquals(Collections.singletonList("BuyerName"), selectProperties);
  }

  @Test
  public void testQueryLeadingQuestionMarkCleaned() {
    String query = SapODataConfigBuilder.builder(VALID_CONFIG)
      .setQuery("???$top=2&$select=By?yerName,Surnam?")
      .build()
      .getQuery();

    Assert.assertEquals("$top=2&$select=By?yerName,Surnam?", query);
  }
}
