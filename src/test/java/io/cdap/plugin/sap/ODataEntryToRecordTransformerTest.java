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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.sap.odata.ODataEntity;
import io.cdap.plugin.sap.transformer.ODataEntryToRecordTransformer;
import org.apache.olingo.odata2.api.edm.EdmSimpleTypeException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * {@link ODataEntryToRecordTransformer} test.
 */
public class ODataEntryToRecordTransformerTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testTransformOData2Types() throws EdmSimpleTypeException {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("binary", Schema.of(Schema.Type.BYTES)),
                                    Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
                                    Schema.Field.of("byte", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("datetime", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                                    Schema.Field.of("datetime_millis", Schema.of(Schema.LogicalType.TIMESTAMP_MILLIS)),
                                    Schema.Field.of("decimal", Schema.decimalOf(4, 2)),
                                    Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("single", Schema.of(Schema.Type.FLOAT)),
                                    Schema.Field.of("guid", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("int16", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("int32", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("int64", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("sbyte", Schema.of(Schema.Type.INT)),
                                    Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("time", Schema.of(Schema.LogicalType.TIME_MICROS)),
                                    Schema.Field.of("time_millis", Schema.of(Schema.LogicalType.TIME_MILLIS)),
                                    Schema.Field.of("datetimeoffset", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("null", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    LocalTime time = LocalTime.now(ZoneOffset.UTC);
    StructuredRecord expected = StructuredRecord.builder(schema)
      .set("binary", "some bytes".getBytes())
      .set("boolean", true)
      .set("byte", (int) Byte.MAX_VALUE)
      .setTimestamp("datetime", ZonedDateTime.now(ZoneOffset.UTC))
      .setTimestamp("datetime_millis", ZonedDateTime.now(ZoneOffset.UTC))
      .setDecimal("decimal", new BigDecimal("12.34"))
      .set("double", Double.MAX_VALUE)
      .set("single", Float.MAX_VALUE)
      .set("guid", UUID.randomUUID().toString())
      .set("int16", (int) Short.MAX_VALUE)
      .set("int32", Integer.MAX_VALUE)
      .set("int64", Long.MAX_VALUE)
      .set("sbyte", (int) Byte.MIN_VALUE)
      .set("string", "Some String")
      .setTime("time", time)
      .setTime("time_millis", time)
      .set("datetimeoffset", "2002-10-10T17:00:01+01:00")
      .set("null", null)
      .build();

    Calendar timeCalendar = Calendar.getInstance();
    timeCalendar.set(Calendar.HOUR_OF_DAY, time.getHour());
    timeCalendar.set(Calendar.MINUTE, time.getMinute());
    timeCalendar.set(Calendar.SECOND, time.getSecond());
    timeCalendar.set(Calendar.MILLISECOND, (int) TimeUnit.NANOSECONDS.toMillis(time.getNano()));
    ODataEntity entity = ODataEntityBuilder.builder()
      .setBinary("binary", expected.<byte[]>get("binary"))
      .setBoolean("boolean", expected.get("boolean"))
      .setByte("byte", expected.<Number>get("byte").byteValue())
      .setDateTime("datetime", GregorianCalendar.from(expected.getTimestamp("datetime")))
      .setDateTime("datetime_millis", GregorianCalendar.from(expected.getTimestamp("datetime_millis")))
      .setDecimal("decimal", expected.getDecimal("decimal"))
      .setDouble("double", expected.<Double>get("double"))
      .setSingle("single", expected.<Float>get("single"))
      .setGuid("guid", UUID.fromString(expected.get("guid")))
      .setInt16("int16", expected.<Number>get("int16").shortValue())
      .setInt32("int32", expected.<Integer>get("int32"))
      .setInt64("int64", expected.<Long>get("int64"))
      .setSByte("sbyte", expected.<Number>get("sbyte").byteValue())
      .setString("string", expected.get("string"))
      .setTime("time", timeCalendar)
      .setTime("time_millis", timeCalendar)
      .setDateTimeOffset("datetimeoffset", expected.<String>get("datetimeoffset"))
      .build();

    ODataEntryToRecordTransformer transformer = new ODataEntryToRecordTransformer(schema);
    StructuredRecord transformed = transformer.transform(entity);

    Assert.assertArrayEquals(expected.<byte[]>get("binary"), transformed.get("binary"));
    Assert.assertEquals(expected.<Boolean>get("boolean"), transformed.get("boolean"));
    Assert.assertEquals(expected.<Byte>get("byte"), transformed.get("byte"));
    Assert.assertEquals(expected.getTimestamp("datetime"), transformed.getTimestamp("datetime"));
    Assert.assertEquals(expected.getTimestamp("datetime_millis"), transformed.getTimestamp("datetime_millis"));
    Assert.assertEquals(expected.getDecimal("decimal"), transformed.getDecimal("decimal"));
    Assert.assertEquals(expected.<Double>get("double"), transformed.get("double"), 0.00001);
    Assert.assertEquals(expected.<Float>get("single"), transformed.<Float>get("single"), 0.00001);
    Assert.assertEquals(expected.<String>get("guid"), transformed.get("guid"));
    Assert.assertEquals(expected.<Short>get("int16"), transformed.get("int16"));
    Assert.assertEquals(expected.<Integer>get("int32"), transformed.get("int32"));
    Assert.assertEquals(expected.<Long>get("int64"), transformed.get("int64"));
    Assert.assertEquals(expected.<Byte>get("sbyte"), transformed.get("sbyte"));
    Assert.assertEquals(expected.<String>get("string"), transformed.get("string"));
    Assert.assertEquals(expected.getTime("time"), transformed.getTime("time"));
    Assert.assertEquals(expected.getTime("time_millis"), transformed.getTime("time_millis"));
    Assert.assertEquals(expected.<String>get("datetimeoffset"), transformed.get("datetimeoffset"));
    Assert.assertNull(transformed.get("null"));
  }
}
