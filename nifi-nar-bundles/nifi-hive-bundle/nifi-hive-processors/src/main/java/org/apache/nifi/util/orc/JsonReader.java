package org.apache.nifi.util.orc;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonStreamParser;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * Created by sterligovak on 21.04.17.
 */
public class JsonReader implements RecordReader {
    private final TypeDescription schema;
    private final JsonStreamParser parser;
    private final JsonConverter[] converters;
    private long rowNumber = 0;

    interface JsonConverter {
        void convert(JsonElement value, ColumnVector vect, int row);
    }

    static class BooleanColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                LongColumnVector vector = (LongColumnVector) vect;
                vector.vector[row] = value.getAsBoolean() ? 1 : 0;
            }
        }
    }

    static class LongColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                LongColumnVector vector = (LongColumnVector) vect;
                vector.vector[row] = value.getAsLong();
            }
        }
    }

    static class DoubleColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                DoubleColumnVector vector = (DoubleColumnVector) vect;
                vector.vector[row] = value.getAsDouble();
            }
        }
    }

    static class StringColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                BytesColumnVector vector = (BytesColumnVector) vect;
                byte[] bytes = value.getAsString().getBytes(StandardCharsets.UTF_8);
                vector.setRef(row, bytes, 0, bytes.length);
            }
        }
    }

    static class BinaryColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                BytesColumnVector vector = (BytesColumnVector) vect;
                String binStr = value.getAsString();
                byte[] bytes = new byte[binStr.length() / 2];
                for (int i = 0; i < bytes.length; ++i) {
                    bytes[i] = (byte) Integer.parseInt(binStr.substring(i * 2, i * 2 + 2), 16);
                }
                vector.setRef(row, bytes, 0, bytes.length);
            }
        }
    }

    static class TimestampColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                TimestampColumnVector vector = (TimestampColumnVector) vect;
                vector.set(row, Timestamp.valueOf(value.getAsString()
                        .replaceAll("[TZ]", " ")));
            }
        }
    }

    static class DecimalColumnConverter implements JsonConverter {
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                DecimalColumnVector vector = (DecimalColumnVector) vect;
                vector.vector[row].set(HiveDecimal.create(value.getAsString()));
            }
        }
    }

    static class StructColumnConverter implements JsonConverter {
        private final JsonConverter[] childrenConverters;
        private final List<String> fieldNames;

        public StructColumnConverter(TypeDescription schema) {
            List<TypeDescription> kids = schema.getChildren();
            childrenConverters = new JsonConverter[kids.size()];
            for (int c = 0; c < childrenConverters.length; ++c) {
                childrenConverters[c] = createConverter(kids.get(c));
            }
            fieldNames = schema.getFieldNames();
        }

        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                StructColumnVector vector = (StructColumnVector) vect;
                JsonObject obj = value.getAsJsonObject();
                for (int c = 0; c < childrenConverters.length; ++c) {
                    JsonElement elem = obj.get(fieldNames.get(c));
                    childrenConverters[c].convert(elem, vector.fields[c], row);
                }
            }
        }
    }

    static class ListColumnConverter implements JsonConverter {
        private final JsonConverter childrenConverter;

        public ListColumnConverter(TypeDescription schema) {
            childrenConverter = createConverter(schema.getChildren().get(0));
        }

        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                ListColumnVector vector = (ListColumnVector) vect;
                JsonArray obj = value.getAsJsonArray();
                vector.lengths[row] = obj.size();
                vector.offsets[row] = vector.childCount;
                vector.childCount += vector.lengths[row];
                for (int c = 0; c < obj.size(); ++c) {
                    childrenConverter.convert(obj.get(c), vector.child,
                            (int) vector.offsets[row] + c);
                }
            }
        }
    }

    static class MapColumnConverter implements JsonConverter {
        private final JsonConverter valueConverter;

        public MapColumnConverter(TypeDescription schema) {
            //keyConverter = createConverter(schema.getChildren().get(0));
            valueConverter = createConverter(schema.getChildren().get(1));
        }

        @Override
        public void convert(JsonElement value, ColumnVector vect, int row) {
            if (value == null || value.isJsonNull()) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                MapColumnVector vector = (MapColumnVector) vect;
                JsonObject obj = value.getAsJsonObject();

                vector.lengths[row] = obj.size();
                vector.offsets[row] = vector.childCount;
                vector.childCount += vector.lengths[row];

                BytesColumnVector keys = (BytesColumnVector) vector.keys;

                int c  = 0;
                for(Map.Entry<String, JsonElement> entry : obj.entrySet()){
                    int nestedRow = (int)vector.offsets[row] + c;

                    String k = entry.getKey();
                    byte[] bytes = k.getBytes(StandardCharsets.UTF_8);
                    keys.setRef(nestedRow, bytes, 0, bytes.length);

                    JsonElement v = entry.getValue();
                    valueConverter.convert(v, vector.values, nestedRow);
                    c++;
                }
            }
        }
    }

    static JsonConverter createConverter(TypeDescription schema) {
        switch (schema.getCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return new LongColumnConverter();
            case FLOAT:
            case DOUBLE:
                return new DoubleColumnConverter();
            case CHAR:
            case VARCHAR:
            case STRING:
                return new StringColumnConverter();
            case DECIMAL:
                return new DecimalColumnConverter();
            case TIMESTAMP:
                return new TimestampColumnConverter();
            case BINARY:
                return new BinaryColumnConverter();
            case BOOLEAN:
                return new BooleanColumnConverter();
            case STRUCT:
                return new StructColumnConverter(schema);
            case LIST:
                return new ListColumnConverter(schema);
            case MAP:
                return new MapColumnConverter(schema);
            default:
                throw new IllegalArgumentException("Unhandled type " + schema);
        }
    }

    public JsonReader(InputStream is,
                      TypeDescription schema) throws IOException {
        this.schema = schema;
        parser = new JsonStreamParser(new InputStreamReader(is,
                StandardCharsets.UTF_8));
        if (schema.getCategory() != TypeDescription.Category.STRUCT) {
            throw new IllegalArgumentException("Root must be struct - " + schema);
        }
        List<TypeDescription> fieldTypes = schema.getChildren();
        converters = new JsonConverter[fieldTypes.size()];
        for (int c = 0; c < converters.length; ++c) {
            converters[c] = createConverter(fieldTypes.get(c));
        }
    }

    public boolean nextBatch(VectorizedRowBatch batch) throws IOException {
        batch.reset();
        int maxSize = batch.getMaxSize();
        List<String> fieldNames = schema.getFieldNames();
        while (parser.hasNext() && batch.size < maxSize) {
            JsonObject elem = parser.next().getAsJsonObject();
            for (int c = 0; c < converters.length; ++c) {
                // look up each field to see if it is in the input, otherwise
                // set it to null.
                JsonElement field = elem.get(fieldNames.get(c));
                if (field == null) {
                    batch.cols[c].noNulls = false;
                    batch.cols[c].isNull[batch.size] = true;
                } else {
                    converters[c].convert(field, batch.cols[c], batch.size);
                }
            }
            batch.size++;
        }
        rowNumber += batch.size;
        return batch.size != 0;
    }

    @Override
    public long getRowNumber() throws IOException {
        return rowNumber;
    }

    @Override
    public float getProgress() throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void seekToRow(long rowCount) throws IOException {
        throw new UnsupportedOperationException("Seek is not supported by JsonReader");
    }
}
