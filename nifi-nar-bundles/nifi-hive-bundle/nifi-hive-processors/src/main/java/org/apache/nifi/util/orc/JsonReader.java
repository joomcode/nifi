package org.apache.nifi.util.orc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.RecordReader;

import java.io.*;

public class JsonReader implements RecordReader {
    private final BufferedReader reader;
    private final JSONToOrcConverter converter;

    private JSONObject currentElement = null;
    private long rowNumber = 0;

    public JsonReader(InputStream is,
                      JSONToOrcConverter converter) throws IOException {
        this.converter = converter;
        this.reader = new BufferedReader(new InputStreamReader(is));
    }

    @Override
    public boolean nextBatch(VectorizedRowBatch batch) throws IOException {
        batch.reset();
        int maxSize = batch.getMaxSize();
        if (currentElement != null){
            converter.writeJsonObject(currentElement, batch);
            currentElement = null;
            batch.size++;
        }

        try {
            String line;
            while (batch.size < maxSize &&
                    (line = this.reader.readLine()) != null) {
                currentElement = JSONObject.parseObject(line, Feature.OrderedField);
                if (currentElement != null) {
                    converter.writeJsonObject(currentElement, batch);
                    currentElement = null;
                    batch.size++;
                }
            }
        } catch (ArrayIndexOutOfBoundsException e){
            // json object was too large, we will process it on next call
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
