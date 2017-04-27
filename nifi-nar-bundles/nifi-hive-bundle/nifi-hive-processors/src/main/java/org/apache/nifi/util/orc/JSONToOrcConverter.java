package org.apache.nifi.util.orc;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.TypeDescription;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class JSONToOrcConverter {
    private final TypeDescription schema;
    private final JsonConverter[] converters;

    public JSONToOrcConverter(TypeDescription schema) {
        this.schema = schema;
        if (schema.getCategory() != TypeDescription.Category.STRUCT) {
            throw new IllegalArgumentException("Root must be struct - " + schema);
        }
        List<TypeDescription> fieldTypes = schema.getChildren();
        converters = new JsonConverter[fieldTypes.size()];
        for (int c = 0; c < converters.length; ++c) {
            converters[c] = createConverter(fieldTypes.get(c));
        }
    }

    void writeJsonObject(JSONObject elem, VectorizedRowBatch batch) {
        List<String> fieldNames = schema.getFieldNames();
        for (int c = 0; c < converters.length; ++c) {
            // look up each field to see if it is in the input, otherwise
            // set it to null.
            Object field = elem.get(fieldNames.get(c));
            if (field == null) {
                batch.cols[c].noNulls = false;
                batch.cols[c].isNull[batch.size] = true;
            } else {
                converters[c].convert(field, batch.cols[c], batch.size);
            }
        }
    }

    private static JsonConverter createConverter(TypeDescription schema) {
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

    private interface JsonConverter {
        void convert(Object value, ColumnVector vect, int row);
    }

    private static class BooleanColumnConverter implements JsonConverter {
        public void convert(Object value, ColumnVector vect, int row) {
            if (value == null) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                LongColumnVector vector = (LongColumnVector) vect;
                vector.vector[row] = (boolean)value ? 1 : 0;
            }
        }
    }

    private static class LongColumnConverter implements JsonConverter {
        public void convert(Object value, ColumnVector vect, int row) {
            if (value == null) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                LongColumnVector vector = (LongColumnVector) vect;

                if (value instanceof Integer){
                    vector.vector[row] = (long)(int)value;
                } else if (value instanceof Long) {
                    vector.vector[row] = (long) value;
                } else if (value instanceof BigDecimal){
                    vector.vector[row] = ((BigDecimal) value).longValue();
                } else {
                    throw new IllegalArgumentException("Expected integer, but got " + value.getClass().getName());
                }
            }
        }
    }

    private static class DoubleColumnConverter implements JsonConverter {
        public void convert(Object value, ColumnVector vect, int row) {
            if (value == null) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                DoubleColumnVector vector = (DoubleColumnVector) vect;
                if (value instanceof Integer){
                    vector.vector[row] = (double)(int)value;
                } else if (value instanceof Long){
                    vector.vector[row] = (double)(long)value;
                } else if (value instanceof Float){
                    vector.vector[row] = (double)(float)value;
                } else if (value instanceof Double) {
                    vector.vector[row] = (double) value;
                } else if (value instanceof BigDecimal){
                    vector.vector[row] = ((BigDecimal) value).doubleValue();
                } else {
                    throw new IllegalArgumentException("Expected integer, but got " + value.getClass().getName());
                }
            }
        }
    }

    private static class StringColumnConverter implements JsonConverter {
        public void convert(Object value, ColumnVector vect, int row) {
            if (value == null) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                BytesColumnVector vector = (BytesColumnVector) vect;
                byte[] bytes = ((String)value).getBytes(StandardCharsets.UTF_8);
                vector.setRef(row, bytes, 0, bytes.length);
            }
        }
    }

    private static class BinaryColumnConverter implements JsonConverter {
        public void convert(Object value, ColumnVector vect, int row) {
            if (value == null) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                BytesColumnVector vector = (BytesColumnVector) vect;
                String binStr = (String)value;
                byte[] bytes = new byte[binStr.length() / 2];
                for (int i = 0; i < bytes.length; ++i) {
                    bytes[i] = (byte) Integer.parseInt(binStr.substring(i * 2, i * 2 + 2), 16);
                }
                vector.setRef(row, bytes, 0, bytes.length);
            }
        }
    }

    private static class TimestampColumnConverter implements JsonConverter {
        public void convert(Object value, ColumnVector vect, int row) {
            if (value == null) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                TimestampColumnVector vector = (TimestampColumnVector) vect;
                vector.set(row, Timestamp.valueOf(
                        ((String)value).replaceAll("[TZ]", " "))
                );
            }
        }
    }

    private static class DecimalColumnConverter implements JsonConverter {
        public void convert(Object value, ColumnVector vect, int row) {
            if (value == null) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                DecimalColumnVector vector = (DecimalColumnVector) vect;
                vector.vector[row].set(HiveDecimal.create((String)value));
            }
        }
    }

    private static class StructColumnConverter implements JsonConverter {
        private final JsonConverter[] childrenConverters;
        private final List<String> fieldNames;

        StructColumnConverter(TypeDescription schema) {
            List<TypeDescription> kids = schema.getChildren();
            childrenConverters = new JsonConverter[kids.size()];
            for (int c = 0; c < childrenConverters.length; ++c) {
                childrenConverters[c] = createConverter(kids.get(c));
            }
            fieldNames = schema.getFieldNames();
        }

        public void convert(Object value, ColumnVector vect, int row) {
            if (value == null) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                StructColumnVector vector = (StructColumnVector) vect;
                JSONObject obj = (JSONObject)value;
                for (int c = 0; c < childrenConverters.length; ++c) {
                    Object elem = obj.get(fieldNames.get(c));
                    childrenConverters[c].convert(elem, vector.fields[c], row);
                }
            }
        }
    }

    private static class ListColumnConverter implements JsonConverter {
        private final JsonConverter childrenConverter;

        ListColumnConverter(TypeDescription schema) {
            childrenConverter = createConverter(schema.getChildren().get(0));
        }

        public void convert(Object value, ColumnVector vect, int row) {
            if (value == null) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                ListColumnVector vector = (ListColumnVector) vect;
                JSONArray obj = (JSONArray)value;
                vector.lengths[row] = obj.size();
                vector.offsets[row] = vector.childCount;
                vector.childCount += vector.lengths[row];
                for (int c = 0; c < obj.size(); ++c) {
                    int nestedRowId = (int) vector.offsets[row] + c;
                    childrenConverter.convert(obj.get(c), vector.child, nestedRowId);
                }
            }
        }
    }

    private static class MapColumnConverter implements JsonConverter {
        private final JsonConverter valueConverter;

        MapColumnConverter(TypeDescription schema) {
            valueConverter = createConverter(schema.getChildren().get(1));
        }

        @Override
        public void convert(Object value, ColumnVector vect, int row) {
            if (value == null) {
                vect.noNulls = false;
                vect.isNull[row] = true;
            } else {
                MapColumnVector vector = (MapColumnVector) vect;
                JSONObject obj = (JSONObject)value;

                vector.lengths[row] = obj.size();
                vector.offsets[row] = vector.childCount;
                vector.childCount += vector.lengths[row];

                BytesColumnVector keys = (BytesColumnVector) vector.keys;

                int c  = 0;
                for(Map.Entry<String, Object> entry : obj.entrySet()){
                    int nestedRow = (int)vector.offsets[row] + c;

                    String k = entry.getKey();
                    byte[] bytes = k.getBytes(StandardCharsets.UTF_8);
                    keys.setRef(nestedRow, bytes, 0, bytes.length);

                    Object v = entry.getValue();
                    valueConverter.convert(v, vector.values, nestedRow);
                    c++;
                }
            }
        }
    }
}
