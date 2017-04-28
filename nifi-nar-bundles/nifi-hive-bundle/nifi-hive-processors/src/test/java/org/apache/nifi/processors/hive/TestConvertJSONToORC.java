package org.apache.nifi.processors.hive;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.orc.TypeDescription;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by sterligovak on 21.04.17.
 */
public class TestConvertJSONToORC {
    private ConvertJSONToORC processor;
    private TestRunner runner;

    @Before
    public void setUp() throws Exception {
        processor = new ConvertJSONToORC();
        runner = TestRunners.newTestRunner(processor);
    }


    @Test
    public void test_Setup() throws Exception {

    }

    @Test
    public void test_basicConversion() throws IOException {
        runner.assertNotValid();
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConvertJSONToORC.ORC_SCHEMA, "struct<id:string>");
        runner.assertValid();

        Map<String, String> attributes = ImmutableMap.of(CoreAttributes.FILENAME.key(), "test.json");
        runner.enqueue(streamFor("{\"id\": \"hello\"}"), attributes);
        runner.run();

        runner.assertTransferCount(ConvertJSONToORC.REL_SUCCESS, 1);
        runner.assertTransferCount(ConvertJSONToORC.REL_FAILURE, 0);

        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(ConvertAvroToORC.REL_SUCCESS).get(0);
        assertEquals("1", resultFlowFile.getAttribute(ConvertAvroToORC.RECORD_COUNT_ATTRIBUTE));
        assertEquals("test.orc", resultFlowFile.getAttribute(CoreAttributes.FILENAME.key()));
        byte[] resultContents = runner.getContentAsByteArray(resultFlowFile);
        FileOutputStream fos = new FileOutputStream("target/test1.orc");
        fos.write(resultContents);
        fos.flush();
        fos.close();

        TypeDescription description = TypeDescription.fromString("struct<id:string>");
        TypeInfo schema = NiFiOrcUtils.toTypeInfo(description);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        Reader reader = OrcFile.createReader(new Path("target/test1.orc"),
                OrcFile.readerOptions(conf)
                        .filesystem(fs)
        );
        RecordReader rows = reader.rows();
        Object o = rows.next(null);
        assertNotNull(o);
        assertTrue(o instanceof OrcStruct);
        StructObjectInspector inspector = (StructObjectInspector) OrcStruct.createObjectInspector(schema);

        // Check some fields in the first row
        Object stringFieldObject = inspector.getStructFieldData(o, inspector.getStructFieldRef("id"));
        assertTrue(stringFieldObject instanceof Text);
        assertEquals("hello", stringFieldObject.toString());
    }

    @Test
    public void test_primitiveTypes() throws CharacterCodingException {
        runner.assertNotValid();
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConvertJSONToORC.ORC_SCHEMA, "struct<" +
                "ti:tinyint," +
                "si:smallint," +
                "i:int," +
                "l:bigint," +
                "f:float," +
                "d:double," +
                "s:string," +
                "ch:char(1)," +
                "vch:varchar(50)," +
                "b:binary," +
                "ts:timestamp" +
                ">");
        runner.assertValid();

        Map<String, String> attributes = ImmutableMap.of(CoreAttributes.FILENAME.key(), "test.json");
        runner.enqueue(streamFor("{\"ti\":0,\"si\":1,\"i\":2,\"l\":3,\"f\":4.0,\"d\":5.0,\"s\":\"string\",\"ch\":\"c\",\"vch\":\"varchar\",\"b\":\"01010101\",\"ts\":\"2017-04-27 10:00:00\"}"), attributes);
        runner.run();

        runner.assertTransferCount(ConvertJSONToORC.REL_SUCCESS, 1);
        runner.assertTransferCount(ConvertJSONToORC.REL_FAILURE, 0);
    }

    @Test
    public void test() throws CharacterCodingException {
        String schema = "struct<id:string,\n" +
                "type:string,\n" +
                "version:int,\n" +
                "serverts:bigint,\n" +
                "device_id:string,\n" +
                "device_ceid:string,\n" +
                "device_clientts:bigint,\n" +
                "device_sessionid:string,\n" +
                "device_ip:string,\n" +
                "device_ipcountry:string,\n" +
                "device_version_appversion:string,\n" +
                "device_version_ostype:string,\n" +
                "device_version_osversion:string,\n" +
                "device_version_hardwaretype:string,\n" +
                "device_version_hardwaremodel:string,\n" +
                "device_envid:string,\n" +
                "device_experiments:map<string,string>,\n" +
                "device_cohort:bigint,\n" +
                "device_relativets:bigint,\n" +
                "user_userid:string,\n" +
                "user_gender:string,\n" +
                "payload_source:string,\n" +
                "payload_price:double,\n" +
                "payload_newcard:boolean,\n" +
                "payload_result:string,\n" +
                "payload_params:map<string,string>,\n" +
                "payload_productid:string,\n" +
                "payload_variantsavailable:boolean,\n" +
                "payload_variantselected:boolean,\n" +
                "payload_location:string,\n" +
                "payload_descriptionshown:boolean,\n" +
                "payload_discountshown:boolean,\n" +
                "payload_screenlocation_rowsperscreen:int,\n" +
                "payload_screenlocation_columnsperscreen:int,\n" +
                "payload_screenlocation_screenrow:int,\n" +
                "payload_screenlocation_screencolumn:int,\n" +
                "payload_sinceopenms:bigint,\n" +
                "payload_productgroupid:string,\n" +
                "payload_mode:string,\n" +
                "payload_trigger:string,\n" +
                "payload_like:boolean,\n" +
                "payload_stars:int,\n" +
                "payload_type:string,\n" +
                "payload_via:string,\n" +
                "payload_categoryid:string,\n" +
                "payload_tags:array<string>,\n" +
                "payload_position:int,\n" +
                "payload_context:string,\n" +
                "payload_pushid:string,\n" +
                "payload_actiontitle:string,\n" +
                "payload_actionurl:string,\n" +
                "payload_active:boolean,\n" +
                "payload_productvariantid:string,\n" +
                "payload_ordergroupid:string,\n" +
                "payload_orderid:string,\n" +
                "payload_merchantid:string,\n" +
                "payload_storeid:string,\n" +
                "payload_msrprice:double,\n" +
                "payload_shippingprice:double,\n" +
                "payload_productprice:double,\n" +
                "payload_totalprice:double,\n" +
                "payload_quantity:int,\n" +
                "payload_errortype:string,\n" +
                "payload_errormessage:string,\n" +
                "payload_country:string,\n" +
                "payload_enabled:boolean,\n" +
                "payload_numberlength:int,\n" +
                "payload_query:string,\n" +
                "payload_queryfilters:array<struct<id:string,categories:array<string>,moneyRange_min:double,moneyRange_max:double,moneyRange_currencyName:string>>,\n" +
                "payload_querysortings:array<struct<fieldName:string,order:string>>,\n" +
                "payload_numresults:int,\n" +
                "payload_discountinfo_type:int,\n" +
                "payload_discountinfo_subtype:int,\n" +
                "payload_discountinfo_discount:double,\n" +
                "payload_tickettype:string,\n" +
                "payload_text:string,\n" +
                "payload_starrating:int,\n" +
                "payload_isanonymous:boolean,\n" +
                "payload_edit:boolean,\n" +
                "payload_address_id:string,\n" +
                "payload_address_createdtimems:bigint,\n" +
                "payload_address_updatedtimems:bigint,\n" +
                "payload_address_firstname:string,\n" +
                "payload_address_middlename:string,\n" +
                "payload_address_lastname:string,\n" +
                "payload_address_email:string,\n" +
                "payload_address_country:string,\n" +
                "payload_address_state:string,\n" +
                "payload_address_city:string,\n" +
                "payload_address_street1: string,\n" +
                "payload_address_street2: string,\n" +
                "payload_address_zip:string,\n" +
                "payload_address_phone:string,\n" +
                "payload_provider:string,\n" +
                "payload_shipping:double,\n" +
                "payload_offers:string,\n" +
                "payload_coupons:boolean,\n" +
                "payload_orders:boolean,\n" +
                "payload_userid:string,\n" +
                "payload_deliverydurationid:string,\n" +
                "device_prefcountry:string,\n" +
                "contexts:map<string,struct<activationTs:bigint,strings:map<string,string>,ints:map<string,int>,floats:map<string,double>,bools:map<string,boolean>>>,\n" +
                "device_version_apptype:string,\n" +
                "payload_queryfiltersmap:map<string,struct<id:string,categories:array<string>,moneyRange_min:double,moneyRange_max:double,moneyRange_currencyName:string>>,\n" +
                "device_utmoriginal:map<string,string>,\n" +
                "device_utmretarget:map<string,string>,\n" +
                "payload_discountinfo_coupontypes:array<string>,\n" +
                "device_ephemeral:boolean,\n" +
                "payload_initialquery:string,\n" +
                "payload_enteredquery:string,\n" +
                "payload_submittedquery:string,\n" +
                "payload_durationms:bigint,\n" +
                "payload_suggestclicked:boolean,\n" +
                "payload_suggestposition:int,\n" +
                "payload_suggestcategory:string,\n" +
                "payload_reasonanswerids:array<string>,\n" +
                "payload_photoscount:int,\n" +
                "device_analyticsid:string,\n" +
                "payload_paymenttype:string,\n" +
                "payload_email:string,\n" +
                "payload_link:string,\n" +
                "payload_originallink:string,\n" +
                "user_ordercount:bigint,\n" +
                "user_gmv:double,\n" +
                "user_segment:string>";

        String data = "{\"id\":\"1493030434583561932-108-101-582-1571650743\",\"type\":\"productPreview\",\"serverTs\":1493030434583,\"device_id\":\"1493029393919718957-42-51-26312-187775008\",\"device_ceId\":\"0f131517-5bd9-4ad2-9069-c15a972077f5\",\"device_clientTs\":1493033972202,\"device_sessionId\":\"8124439b-5da1-4f87-bdca-6539273c1f76\",\"device_ip\":\"81.162.240.64\",\"device_ipCountry\":\"UA\",\"device_version_appVersion\":\"1.16.2\",\"device_version_osType\":\"android\",\"device_version_osVersion\":\"5.1\",\"device_version_hardwareType\":\"phone\",\"device_version_hardwareModel\":\"ASSISTANT AS-5431\",\"device_version_appType\":\"android\",\"device_experiments\":{\"builtinTracking\":\"on\",\"cardio\":\"disabled\",\"control10\":\"control6\",\"control50\":\"controlA\",\"enable-paybox\":\"disabled\",\"groupsInSocialNetworks\":\"groupsInSocialNetworks\",\"helpshift\":\"enabled\",\"hide-category-tabs\":\"enabled\",\"invite-friend-link-type\":\"branch\",\"merchant-offers\":\"enabled\",\"no-daily-push\":\"baseline\",\"product-discount\":\"price_exponent_discount_progressive\",\"productPreviewIndicator\":\"rating\",\"rankerPreviewLimit\":\"test\",\"searchEngine\":\"detectum1\",\"show-all-offers-tab\":\"baseline\",\"web-payments-enabled\":\"enabled\"},\"device_cohort\":1492981200000,\"device_relativeTs\":1040664,\"device_prefCountry\":\"UA\",\"user_userId\":\"1493029393919743417-43-53-26312-4132024335\",\"user_gender\":\"female\",\"user_orderCount\":0,\"user_gmv\":0,\"user_segment\":\"New\",\"payload_productid\":\"1476251364994106319-48-1-26312-3501884824\",\"contexts\":{\"pg\":{\"strings\":{\"categoryId\":\"1482223890692828544-248-2-629-1399925912\",\"groupId\":\"1486738636965938834-87-5-582-779136106\",\"groupScope\":\"global\",\"groupType\":\"offer\",\"merchantId\":\"1479114284851825003-17-11-118-3555156886\",\"storeId\":\"1463735309719057672-129-3-553-2425712568\"},\"ints\":{\"position\":209,\"ratingsCount\":5},\"floats\":{\"origPrice\":6,\"rating\":3.6}}}}";

        runner.assertNotValid();
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConvertJSONToORC.ORC_SCHEMA, schema);
        runner.assertValid();

        Map<String, String> attributes = ImmutableMap.of(CoreAttributes.FILENAME.key(), "test.json");
        runner.enqueue(streamFor(data), attributes);
        runner.run();

        runner.assertTransferCount(ConvertJSONToORC.REL_SUCCESS, 1);
        runner.assertTransferCount(ConvertJSONToORC.REL_FAILURE, 0);
    }

    @Test
    public void test_convert_multiple_entries() throws IOException {
        runner.assertNotValid();
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConvertJSONToORC.ORC_SCHEMA, "struct<id:string>");
        runner.assertValid();

        Map<String, String> attributes = ImmutableMap.of(CoreAttributes.FILENAME.key(), "test1.json");
        runner.enqueue(streamFor("{\"id\": \"1\"}\n{\"id\": \"2\"}"), attributes);
        runner.run();

        runner.assertTransferCount(ConvertJSONToORC.REL_SUCCESS, 1);
        runner.assertTransferCount(ConvertJSONToORC.REL_FAILURE, 0);

        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(ConvertAvroToORC.REL_SUCCESS).get(0);
        assertEquals("2", resultFlowFile.getAttribute(ConvertAvroToORC.RECORD_COUNT_ATTRIBUTE));
        assertEquals("test1.orc", resultFlowFile.getAttribute(CoreAttributes.FILENAME.key()));

        byte[] resultContents = runner.getContentAsByteArray(resultFlowFile);
        FileOutputStream fos = new FileOutputStream("target/test1.orc");
        fos.write(resultContents);
        fos.flush();
        fos.close();

        TypeDescription description = TypeDescription.fromString("struct<id:string>");
        TypeInfo schema = NiFiOrcUtils.toTypeInfo(description);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        Reader reader = OrcFile.createReader(new Path("target/test1.orc"),
                OrcFile.readerOptions(conf)
                        .filesystem(fs)
        );
        RecordReader rows = reader.rows();
        Object o = rows.next(null);
        assertNotNull(o);
        assertTrue(o instanceof OrcStruct);
        StructObjectInspector inspector = (StructObjectInspector) OrcStruct.createObjectInspector(schema);

        // Check some fields in the first row
        Object stringFieldObject = inspector.getStructFieldData(o, inspector.getStructFieldRef("id"));
        assertTrue(stringFieldObject instanceof Text);
        assertEquals("1", stringFieldObject.toString());

        // Check next row
        o = rows.next(o);
        assertNotNull(o);
        assertTrue(o instanceof OrcStruct);
        inspector = (StructObjectInspector) OrcStruct.createObjectInspector(schema);

        // Check some fields in the first row
        stringFieldObject = inspector.getStructFieldData(o, inspector.getStructFieldRef("id"));
        assertTrue(stringFieldObject instanceof Text);
        assertEquals("2", stringFieldObject.toString());
    }

    @Test
    public void test_complexType() throws IOException {
        final String stringSchema = "struct<entry: struct<id: int,val: string>,str: string>";
        runner.assertNotValid();
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConvertJSONToORC.ORC_SCHEMA, stringSchema);
        runner.assertValid();

        Map<String, String> attributes = ImmutableMap.of(CoreAttributes.FILENAME.key(), "test.json");
        runner.enqueue(streamFor("{\"entry\": {\"id\": 10, \"val\": \"world\"}, \"str\": \"value\"}"), attributes);
        runner.run();

        runner.assertTransferCount(ConvertJSONToORC.REL_SUCCESS, 1);
        runner.assertTransferCount(ConvertJSONToORC.REL_FAILURE, 0);

        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(ConvertAvroToORC.REL_SUCCESS).get(0);
        assertEquals("1", resultFlowFile.getAttribute(ConvertAvroToORC.RECORD_COUNT_ATTRIBUTE));
        assertEquals("test.orc", resultFlowFile.getAttribute(CoreAttributes.FILENAME.key()));
        byte[] resultContents = runner.getContentAsByteArray(resultFlowFile);
        FileOutputStream fos = new FileOutputStream("target/test1.orc");
        fos.write(resultContents);
        fos.flush();
        fos.close();

        TypeDescription description = TypeDescription.fromString(stringSchema.replaceAll("\\s+", ""));
        TypeInfo schema = NiFiOrcUtils.toTypeInfo(description);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        Reader reader = OrcFile.createReader(new Path("target/test1.orc"),
                OrcFile.readerOptions(conf)
                        .filesystem(fs)
        );
        RecordReader rows = reader.rows();
        Object o = rows.next(null);
        assertNotNull(o);
        assertTrue(o instanceof OrcStruct);
        StructObjectInspector inspector = (StructObjectInspector) OrcStruct.createObjectInspector(schema);

        // Check some fields in the first row
        Object structFieldObject = inspector.getStructFieldData(o, inspector.getStructFieldRef("entry"));
        assertTrue(structFieldObject instanceof OrcStruct);

        StructObjectInspector nestedEntryInspector = (StructObjectInspector)OrcStruct.createObjectInspector(((StructTypeInfo)schema).getStructFieldTypeInfo("entry"));
        Object idField = nestedEntryInspector.getStructFieldData(structFieldObject, nestedEntryInspector.getStructFieldRef("id"));
        assertTrue(idField instanceof IntWritable);
        assertEquals(10, ((IntWritable)idField).get());

        Object valueField =  nestedEntryInspector.getStructFieldData(structFieldObject, nestedEntryInspector.getStructFieldRef("val"));
        assertTrue(valueField instanceof Text);
        assertEquals("world", valueField.toString());

        Object stringFieldObject = inspector.getStructFieldData(o, inspector.getStructFieldRef("str"));
        assertTrue(stringFieldObject instanceof Text);
        assertEquals("value", stringFieldObject.toString());
    }

    @Test
    public void test_simpleMap() throws IOException {
        final String stringSchema = "struct<map:map<string,string>>";
        runner.assertNotValid();
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConvertJSONToORC.ORC_SCHEMA, stringSchema);
        runner.assertValid();

        Map<String, String> attributes = ImmutableMap.of(CoreAttributes.FILENAME.key(), "test.json");

        StringBuilder builder = new StringBuilder();
        for(int i = 0; i < 1000; i++){
            builder.append(String.format("{\"map\": {\"id\": \"hello%s\", \"val\": \"world%s\"}}\n", i, i));
        }

        runner.enqueue(streamFor(builder.toString()), attributes);
        runner.run();

        runner.assertTransferCount(ConvertJSONToORC.REL_SUCCESS, 1);
        runner.assertTransferCount(ConvertJSONToORC.REL_FAILURE, 0);

        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(ConvertAvroToORC.REL_SUCCESS).get(0);
        assertEquals("1000", resultFlowFile.getAttribute(ConvertAvroToORC.RECORD_COUNT_ATTRIBUTE));
        assertEquals("test.orc", resultFlowFile.getAttribute(CoreAttributes.FILENAME.key()));
        byte[] resultContents = runner.getContentAsByteArray(resultFlowFile);
        FileOutputStream fos = new FileOutputStream("target/test1.orc");
        fos.write(resultContents);
        fos.flush();
        fos.close();

        TypeDescription description = TypeDescription.fromString(stringSchema.replaceAll("\\s+", ""));
        TypeInfo schema = NiFiOrcUtils.toTypeInfo(description);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        Reader reader = OrcFile.createReader(new Path("target/test1.orc"),
                OrcFile.readerOptions(conf)
                        .filesystem(fs)
        );
        RecordReader rows = reader.rows();

        for(int i = 0; i < 1000; i++) {
            Object o = rows.next(null);
            assertNotNull(o);
            assertTrue(o instanceof OrcStruct);
            StructObjectInspector inspector = (StructObjectInspector) OrcStruct.createObjectInspector(schema);

            // Check some fields in the first row
            Object mapFieldObject = inspector.getStructFieldData(o, inspector.getStructFieldRef("map"));
            assertTrue(mapFieldObject instanceof Map);

            assertEquals(new Text("hello" + i), ((Map) mapFieldObject).get(new Text("id")));
            assertEquals(new Text("world" + i), ((Map) mapFieldObject).get(new Text("val")));
        }
    }

    @Test
    public void test_list_large() throws IOException {
        final String stringSchema = "struct<list:array<string>>";
        runner.assertNotValid();
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConvertJSONToORC.ORC_SCHEMA, stringSchema);
        runner.assertValid();

        Map<String, String> attributes = ImmutableMap.of(CoreAttributes.FILENAME.key(), "test.json");

        StringBuilder builder = new StringBuilder();
        for(int i = 0; i < 2000; i++){
            builder.append("{\"list\": [\"val1\", \"val2\"]}\n");
        }

        runner.enqueue(streamFor(builder.toString()), attributes);
        runner.run();

        runner.assertTransferCount(ConvertJSONToORC.REL_SUCCESS, 1);
        runner.assertTransferCount(ConvertJSONToORC.REL_FAILURE, 0);

        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(ConvertAvroToORC.REL_SUCCESS).get(0);
        assertEquals("2000", resultFlowFile.getAttribute(ConvertAvroToORC.RECORD_COUNT_ATTRIBUTE));
    }

    @Test
    public void test_complexMap() throws IOException {
        final String stringSchema = "struct<map:map<string,struct<value:string>>>";
        runner.assertNotValid();
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConvertJSONToORC.ORC_SCHEMA, stringSchema);
        runner.assertValid();

        Map<String, String> attributes = ImmutableMap.of(CoreAttributes.FILENAME.key(), "test.json");
        runner.enqueue(streamFor("{\"map\": {\"id\": {\"value\": \"hello\"}, \"val\": {\"value\": \"world\"}}}"), attributes);
        runner.run();

        runner.assertTransferCount(ConvertJSONToORC.REL_SUCCESS, 1);
        runner.assertTransferCount(ConvertJSONToORC.REL_FAILURE, 0);

        MockFlowFile resultFlowFile = runner.getFlowFilesForRelationship(ConvertAvroToORC.REL_SUCCESS).get(0);
        assertEquals("1", resultFlowFile.getAttribute(ConvertAvroToORC.RECORD_COUNT_ATTRIBUTE));
        assertEquals("test.orc", resultFlowFile.getAttribute(CoreAttributes.FILENAME.key()));
        byte[] resultContents = runner.getContentAsByteArray(resultFlowFile);
        FileOutputStream fos = new FileOutputStream("target/test1.orc");
        fos.write(resultContents);
        fos.flush();
        fos.close();

        TypeDescription description = TypeDescription.fromString(stringSchema.replaceAll("\\s+", ""));
        TypeInfo schema = NiFiOrcUtils.toTypeInfo(description);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        Reader reader = OrcFile.createReader(new Path("target/test1.orc"),
                OrcFile.readerOptions(conf)
                        .filesystem(fs)
        );
        RecordReader rows = reader.rows();
        Object o = rows.next(null);
        assertNotNull(o);
        assertTrue(o instanceof OrcStruct);
        StructObjectInspector inspector = (StructObjectInspector) OrcStruct.createObjectInspector(schema);

        // Check some fields in the first row
        Object mapFieldObject = inspector.getStructFieldData(o, inspector.getStructFieldRef("map"));
        assertTrue(mapFieldObject instanceof Map);

        Object structFieldObject = ((Map)mapFieldObject).get(new Text("id"));
        assertTrue(structFieldObject instanceof OrcStruct);
        assertEquals("{hello}", structFieldObject.toString());

        structFieldObject = ((Map)mapFieldObject).get(new Text("val"));
        assertTrue(structFieldObject instanceof OrcStruct);
        assertEquals("{world}", structFieldObject.toString());
    }

    public static InputStream streamFor(String content) throws CharacterCodingException {
        return streamFor(content, Charset.forName("utf8"));
    }

    public static InputStream streamFor(String content, Charset charset) throws CharacterCodingException {
        return new ByteArrayInputStream(bytesFor(content, charset));
    }

    public static byte[] bytesFor(String content, Charset charset) throws CharacterCodingException {
        CharBuffer chars = CharBuffer.wrap(content);
        CharsetEncoder encoder = charset.newEncoder();
        ByteBuffer buffer = encoder.encode(chars);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }
}
