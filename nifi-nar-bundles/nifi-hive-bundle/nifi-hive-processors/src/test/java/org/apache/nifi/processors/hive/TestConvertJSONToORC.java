package org.apache.nifi.processors.hive;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.orc.TypeDescriptionUtils;
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

        runner.assertAllFlowFilesTransferred(ConvertAvroToORC.REL_SUCCESS, 1);

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
    public void test_convert_multiple_entries() throws IOException {
        runner.assertNotValid();
        runner.setValidateExpressionUsage(false);
        runner.setProperty(ConvertJSONToORC.ORC_SCHEMA, "struct<id:string>");
        runner.assertValid();

        Map<String, String> attributes = ImmutableMap.of(CoreAttributes.FILENAME.key(), "test1.json");
        runner.enqueue(streamFor("{\"id\": \"1\"}\n{\"id\": \"2\"}"), attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertAvroToORC.REL_SUCCESS, 1);

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

        runner.assertAllFlowFilesTransferred(ConvertAvroToORC.REL_SUCCESS, 1);

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
