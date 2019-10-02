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
package io.cdap.plugin.sap.etl;

import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.sap.AnnotationRecordBuilder;
import io.cdap.plugin.sap.SapODataConstants;
import org.apache.olingo.commons.api.edm.annotation.EdmExpression;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;


public class SapOData4MetadataAnnotationsETLTest extends BaseSapODataSourceETLTest {

  private static final String SERVICE_PATH = "/sap/opu/odata/SAP/ZGW100_XX_S2_SRV";
  private static final String ENTITY_SET = "AllDataTypes";
  private static final String DESCRIPTION_TERM = "Core.Description";
  private static final String CURRENCY_TERM = "Measures.ISOCurrency";

  @Before
  public void testSetup() throws Exception {
    wireMockRule.stubFor(WireMock.get(WireMock.urlEqualTo(SERVICE_PATH + "/$metadata"))
                           .willReturn(WireMock.aResponse().withBody(readResourceFile("odata4/metadata.xml"))));

    ResponseDefinitionBuilder jsonResponse = WireMock.aResponse()
      .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
      .withBody(readResourceFile("odata4/AllDataTypes.json"));
    wireMockRule.stubFor(WireMock.get(WireMock.urlMatching(SERVICE_PATH + "/" + ENTITY_SET + ".*"))
                           .willReturn(jsonResponse));
  }

  @Test
  public void testMultipleAnnotations() throws Exception {
    StructuredRecord expectedCurrency = AnnotationRecordBuilder.builder("MultipleAnnotated")
      .setTerm(CURRENCY_TERM)
      .withConstantExpression(EdmExpression.EdmExpressionType.String, "USD")
      .build();

    StructuredRecord expectedDescription = AnnotationRecordBuilder.builder("MultipleAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withConstantExpression(EdmExpression.EdmExpressionType.String, "Annotated property")
      .build();

    Map<String, String> properties = sourceProperties("$select=MultipleAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("MultipleAnnotated");
      assertAnnotationEquals(expectedCurrency, fieldRecord, "measures_isocurrency");
      assertAnnotationEquals(expectedDescription, fieldRecord, "core_description");
    }
  }

  @Test
  public void testNestedAnnotation() throws Exception {
    StructuredRecord nestedAnnotation = AnnotationRecordBuilder.builder("MultipleAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withConstantExpression(EdmExpression.EdmExpressionType.String, "Nested annotation")
      .build();

    StructuredRecord expectedCurrency = AnnotationRecordBuilder.builder("NestedAnnotation")
      .setTerm(CURRENCY_TERM)
      .withConstantExpression(EdmExpression.EdmExpressionType.String, "USD")
      .withAnnotation("core_description", nestedAnnotation)
      .build();

    Map<String, String> properties = sourceProperties("$select=NestedAnnotation");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("NestedAnnotation");
      assertAnnotationEquals(expectedCurrency, fieldRecord, "measures_isocurrency");
    }
  }

  @Test
  public void testApplyExpressionAnnotation() throws Exception {
    StructuredRecord expected = AnnotationRecordBuilder.builder("CanonicalFunctionsProperty")
      .setTerm(DESCRIPTION_TERM)
      .withApplyExpression("odata.concat")
      .withConstantExpression(EdmExpression.EdmExpressionType.String, "Product: ")
      .withConstantExpression(EdmExpression.EdmExpressionType.Path, "String")
      .withConstantExpression(EdmExpression.EdmExpressionType.String, " (")
      .withConstantExpression(EdmExpression.EdmExpressionType.Path, "Int16")
      .withConstantExpression(EdmExpression.EdmExpressionType.String, ")")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=CanonicalFunctionsProperty");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("CanonicalFunctionsProperty");
      Assert.assertNotNull(fieldRecord);
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testAndExpressionAnnotation() throws Exception {
    StructuredRecord expected = AnnotationRecordBuilder.builder("AndAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withLogicalExpression(EdmExpression.EdmExpressionType.And)
      .left(EdmExpression.EdmExpressionType.Path, "Boolean")
      .right(EdmExpression.EdmExpressionType.Path, "Boolean")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=AndAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("AndAnnotated");
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testOrExpressionAnnotation() throws Exception {
    StructuredRecord expected = AnnotationRecordBuilder.builder("OrAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withLogicalExpression(EdmExpression.EdmExpressionType.Or)
      .left(EdmExpression.EdmExpressionType.Path, "AndAnnotated")
      .right(EdmExpression.EdmExpressionType.Path, "Boolean")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=OrAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("OrAnnotated");
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testNotExpressionAnnotation() throws Exception {
    StructuredRecord expected = AnnotationRecordBuilder.builder("NotAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withNotExpression()
      .value(EdmExpression.EdmExpressionType.Path, "AndAnnotated")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=NotAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("NotAnnotated");
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testEqExpressionAnnotation() throws Exception {
    StructuredRecord expected = AnnotationRecordBuilder.builder("EqAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withLogicalExpression(EdmExpression.EdmExpressionType.Eq)
      .left(EdmExpression.EdmExpressionType.Path, "Int16")
      .right(EdmExpression.EdmExpressionType.Path, "Int32")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=EqAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("EqAnnotated");
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testNeExpressionAnnotation() throws Exception {
    StructuredRecord expected = AnnotationRecordBuilder.builder("NeAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withLogicalExpression(EdmExpression.EdmExpressionType.Ne)
      .left(EdmExpression.EdmExpressionType.Path, "Int16")
      .right(EdmExpression.EdmExpressionType.Path, "Int32")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=NeAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("NeAnnotated");
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testGtExpressionAnnotation() throws Exception {
    StructuredRecord expected = AnnotationRecordBuilder.builder("GtAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withLogicalExpression(EdmExpression.EdmExpressionType.Gt)
      .left(EdmExpression.EdmExpressionType.Path, "Int16")
      .right(EdmExpression.EdmExpressionType.Path, "Int32")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=GtAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("GtAnnotated");
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testGeExpressionAnnotation() throws Exception {
    StructuredRecord expected = AnnotationRecordBuilder.builder("GeAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withLogicalExpression(EdmExpression.EdmExpressionType.Ge)
      .left(EdmExpression.EdmExpressionType.Path, "Int16")
      .right(EdmExpression.EdmExpressionType.Path, "Int32")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=GeAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("GeAnnotated");
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testLtExpressionAnnotation() throws Exception {
    StructuredRecord expected = AnnotationRecordBuilder.builder("LtAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withLogicalExpression(EdmExpression.EdmExpressionType.Lt)
      .left(EdmExpression.EdmExpressionType.Path, "Int16")
      .right(EdmExpression.EdmExpressionType.Path, "Int32")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=LtAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("LtAnnotated");
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testLeExpressionAnnotation() throws Exception {
    StructuredRecord expected = AnnotationRecordBuilder.builder("LeAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withLogicalExpression(EdmExpression.EdmExpressionType.Le)
      .left(EdmExpression.EdmExpressionType.Path, "Int32")
      .right(EdmExpression.EdmExpressionType.Path, "Int16")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=LeAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("LeAnnotated");
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testCastExpressionAnnotation() throws Exception {
    StructuredRecord expected = AnnotationRecordBuilder.builder("CastAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withCastExpression("Edm.String")
      .value(EdmExpression.EdmExpressionType.Path, "Int16")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=CastAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("CastAnnotated");
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testCollectionExpressionAnnotation() throws Exception {
    StructuredRecord expected = AnnotationRecordBuilder.builder("CollectionAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withCollectionExpression()
      .withItem(EdmExpression.EdmExpressionType.String, "Product")
      .withItem(EdmExpression.EdmExpressionType.String, "Supplier")
      .withItem(EdmExpression.EdmExpressionType.String, "Customer")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=CollectionAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("CollectionAnnotated");
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testIfExpressionAnnotation() throws Exception {
    StructuredRecord expected = AnnotationRecordBuilder.builder("IfAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withIfExpression()
      .withGuard(EdmExpression.EdmExpressionType.Path, "Boolean")
      .withThen(EdmExpression.EdmExpressionType.String, "Female")
      .withElse(EdmExpression.EdmExpressionType.String, "Male")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=IfAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("IfAnnotated");
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testIsOfExpressionAnnotation() throws Exception {
    StructuredRecord expected = AnnotationRecordBuilder.builder("IsOfAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withIsOfExpression("Edm.Boolean")
      .value(EdmExpression.EdmExpressionType.Path, "Int16")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=IsOfAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("IsOfAnnotated");
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testLabeledElementExpressionAnnotation() throws Exception {
    StructuredRecord expected = AnnotationRecordBuilder.builder("LabeledElementAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withLabeledElementExpression("CustomerFirstName")
      .value(EdmExpression.EdmExpressionType.Path, "String")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=LabeledElementAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("LabeledElementAnnotated");
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testRecordExpressionAnnotation() throws Exception {
    StructuredRecord expected = AnnotationRecordBuilder.builder("RecordAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withRecordExpression()
      .withProperty("GivenName", EdmExpression.EdmExpressionType.Path, "String")
      .withProperty("Age", EdmExpression.EdmExpressionType.Path, "Byte")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=RecordAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("RecordAnnotated");
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testRecordExpressionNestedAnnotation() throws Exception {
    StructuredRecord nestedAnnotation = AnnotationRecordBuilder.builder("RecordNestedAnnotated-nested")
      .setTerm(DESCRIPTION_TERM)
      .withConstantExpression(EdmExpression.EdmExpressionType.String, "Annotation on record")
      .build();

    StructuredRecord expected = AnnotationRecordBuilder.builder("RecordNestedAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withRecordExpression()
      .withProperty("GivenName", EdmExpression.EdmExpressionType.Path, "String")
      .withProperty("Age", EdmExpression.EdmExpressionType.Path, "Byte")
      .withAnnotation("core_description", nestedAnnotation)
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=RecordNestedAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("RecordNestedAnnotated");
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testUrlRefExpressionAnnotation() throws Exception {
    StructuredRecord expected = AnnotationRecordBuilder.builder("UrlRefAnnotated")
      .setTerm(DESCRIPTION_TERM)
      .withUrlRefExpression()
      .value(EdmExpression.EdmExpressionType.String, "http://host/wiki/HowToUse")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=UrlRefAnnotated");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("UrlRefAnnotated");
      assertAnnotationEquals(expected, fieldRecord, "core_description");
    }
  }

  @Test
  public void testExternalTargetingAnnotations() throws Exception {
    StructuredRecord expectedCurrency = AnnotationRecordBuilder.builder("Targeted")
      .setTerm(CURRENCY_TERM)
      .withConstantExpression(EdmExpression.EdmExpressionType.String, "USD")
      .build();

    StructuredRecord expectedDescription = AnnotationRecordBuilder.builder("Targeted")
      .setTerm(DESCRIPTION_TERM)
      .withConstantExpression(EdmExpression.EdmExpressionType.String, "Property annotated with External Targeting")
      .build();

    StructuredRecord expectedValueReferencesList = AnnotationRecordBuilder.builder("Targeted")
      .setTerm("SAP__common.ValueListReferences")
      .withCollectionExpression()
      .withItem(EdmExpression.EdmExpressionType.String, "../dummy1/$metadata")
      .withItem(EdmExpression.EdmExpressionType.String, "../dummy2/$metadata")
      .add()
      .build();

    Map<String, String> properties = sourceProperties("$select=Targeted");
    List<StructuredRecord> records = getPipelineResults(properties);
    Assert.assertEquals(2, records.size());
    for (StructuredRecord actualRecord : records) {
      StructuredRecord fieldRecord = actualRecord.get("Targeted");
      assertAnnotationEquals(expectedCurrency, fieldRecord, "measures_isocurrency");
      assertAnnotationEquals(expectedDescription, fieldRecord, "core_description");
      assertAnnotationEquals(expectedValueReferencesList, fieldRecord, "sap__common_valuelistreferences");
    }
  }

  private void assertAnnotationEquals(StructuredRecord expected, StructuredRecord fieldRecord, String annotation) {
    Assert.assertNotNull(fieldRecord);
    StructuredRecord annotationsRecord = fieldRecord.get(SapODataConstants.METADATA_ANNOTATIONS_FIELD_NAME);
    Assert.assertNotNull(annotationsRecord);
    StructuredRecord actual = annotationsRecord.get(annotation);

    Assert.assertEquals(expected, actual);
  }

  private Map<String, String> sourceProperties(String query) {
    return new ImmutableMap.Builder<String, String>()
      .put(SapODataConstants.ODATA_SERVICE_URL, getServerAddress() + SERVICE_PATH)
      .put(SapODataConstants.INCLUDE_METADATA_ANNOTATIONS, "true")
      .put(SapODataConstants.RESOURCE_PATH, ENTITY_SET)
      .put(SapODataConstants.QUERY, query)
      .build();
  }
}
