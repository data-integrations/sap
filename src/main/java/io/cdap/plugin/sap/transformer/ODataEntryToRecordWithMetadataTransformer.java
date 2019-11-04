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

package io.cdap.plugin.sap.transformer;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.sap.SapODataConstants;
import io.cdap.plugin.sap.odata.ODataAnnotation;
import io.cdap.plugin.sap.odata.ODataEntity;
import io.cdap.plugin.sap.odata.odata2.OData2Annotation;
import io.cdap.plugin.sap.odata.odata4.OData4Annotation;

import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * Transforms {@link ODataEntity} to {@link StructuredRecord} including metadata annotations.
 */
public class ODataEntryToRecordWithMetadataTransformer extends ODataEntryToRecordTransformer {

  private Map<String, List<ODataAnnotation>> fieldNameToAnnotations;
  private OData4AnnotationToRecordTransformer oData4AnnotationToRecordTransformer;

  public ODataEntryToRecordWithMetadataTransformer(Schema schema,
                                                   Map<String, List<ODataAnnotation>> fieldNameToAnnotations) {
    super(schema);
    this.fieldNameToAnnotations = fieldNameToAnnotations;
    this.oData4AnnotationToRecordTransformer = new OData4AnnotationToRecordTransformer();
  }

  /**
   * Transforms given {@link ODataEntity} to {@link StructuredRecord} including metadata annotations.
   *
   * @param oDataEntity ODataEntry to be transformed.
   * @return {@link StructuredRecord} with metadata annotations that corresponds to the given {@link ODataEntity}.
   */
  public StructuredRecord transform(ODataEntity oDataEntity) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      Schema nonNullableSchema = field.getSchema().isNullable() ?
        field.getSchema().getNonNullable() : field.getSchema();
      String fieldName = field.getName();
      Object property = oDataEntity.getProperties().get(fieldName);
      List<ODataAnnotation> annotations = fieldNameToAnnotations.get(fieldName);
      Object extractedValue = annotations == null || annotations.isEmpty()
        ? extractValue(fieldName, property, nonNullableSchema)
        : extractValueMetadataRecord(fieldName, property, nonNullableSchema);
      builder.set(fieldName, extractedValue);
    }
    return builder.build();
  }

  private StructuredRecord extractValueMetadataRecord(String fieldName, Object value, Schema schema) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);

    Schema.Field valueField = schema.getField(SapODataConstants.VALUE_FIELD_NAME);
    Schema valueNonNullableSchema = valueField.getSchema().isNullable()
      ? valueField.getSchema().getNonNullable() : valueField.getSchema();
    builder.set(SapODataConstants.VALUE_FIELD_NAME, extractValue(fieldName, value, valueNonNullableSchema));

    Schema.Field metadataField = schema.getField(SapODataConstants.METADATA_ANNOTATIONS_FIELD_NAME);
    Schema metadataNonNullableSchema = metadataField.getSchema().isNullable()
      ? metadataField.getSchema().getNonNullable() : metadataField.getSchema();
    StructuredRecord metadataRecord = extractMetadataRecord(fieldName, metadataNonNullableSchema);
    builder.set(SapODataConstants.METADATA_ANNOTATIONS_FIELD_NAME, metadataRecord);

    return builder.build();
  }

  private StructuredRecord extractMetadataRecord(String fieldName, Schema schema) {
    List<ODataAnnotation> annotations = fieldNameToAnnotations.get(fieldName);
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    annotations.stream()
      .filter(a -> Objects.nonNull(schema.getField(a.getName())))
      .forEach(a -> builder.set(a.getName(), extractAnnotationValue(a, schema)));
    return builder.build();
  }

  private Object extractAnnotationValue(ODataAnnotation annotation, Schema schema) {
    if (annotation instanceof OData2Annotation) {
      return ((OData2Annotation) annotation).getValue();
    }
    // OData 4 annotations mapped to record
    Schema.Field annotationField = schema.getField(annotation.getName());
    Schema annotationRecordSchema = annotationField.getSchema().isNullable()
      ? annotationField.getSchema().getNonNullable() : annotationField.getSchema();
    return oData4AnnotationToRecordTransformer.transform((OData4Annotation) annotation, annotationRecordSchema);
  }
}
