package com.my.tutorial;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author sabo
 *
 */
public class MyHiveInputFormat extends FileInputFormat<Text, BytesWritable> {

    @Override
    public RecordReader<Text, BytesWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
            throws IOException {
        // TODO Auto-generated method stub
        return new MyHiveRecordReader(split, job);
    }

}
