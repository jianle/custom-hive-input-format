package com.my.tutorial;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.xerial.snappy.SnappyInputStream;

public class MyHiveCombineRecordReader implements RecordReader<Text, BytesWritable> {

    private static final Log LOG = LogFactory.getLog(MyHiveCombineRecordReader.class);

    private long start;
    private long pos;
    private long end;
    private InputStream in;

    public MyHiveCombineRecordReader(CombineFileSplit split, Configuration conf, Reporter report, Integer index)
            throws IOException {

        final Path file = split.getPath(index);
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(file);

        CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(conf);
        final CompressionCodec codec = compressionCodecFactory.getCodec(file);

        if (codec != null) {
            LOG.debug("use conf compressioncodec.");
            in = codec.createInputStream(fileIn);
        } else {
            in = new SnappyInputStream(fileIn);
        }

        this.start = split.getOffset(index);
        this.end = start + split.getLength(index);
        this.pos = start;
    }

    public boolean next(Text key, BytesWritable value) throws IOException {
        byte[] header = new byte[4];
        while (in.read(header, 0, 4) != -1) {
            int bodyLength = bytesToInt(header);
            byte[] body = new byte[bodyLength];
            in.read(body, 0, bodyLength);
            key.set(new Text());
            value.set(body, 0, bodyLength);
            return true;
        }
        return false;
    }

    public Text createKey() {
        return new Text();
    }

    public BytesWritable createValue() {
        return new BytesWritable();
    }

    public long getPos() throws IOException {
        return this.pos;
    }

    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    public float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    private int bytesToInt(byte[] bits) {
        int val;
        val = (int) ((bits[3] & 0xFF) | ((bits[2] & 0xFF) << 8) | ((bits[1] & 0xFF) << 16) | ((bits[0] & 0xFF) << 24));
        return val;
    }
}
