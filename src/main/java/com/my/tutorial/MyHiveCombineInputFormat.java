package com.my.tutorial;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;


public class MyHiveCombineInputFormat extends CombineFileInputFormat<Text, BytesWritable> {
    
    private static final Log LOG = LogFactory.getLog(MyHiveCombineInputFormat.class);
    
    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        InputSplit[] splits =  super.getSplits(job, numSplits);
        
        int i = 0;
        MyHiveInputSplit[] wsplits = new MyHiveInputSplit[splits.length];
        for (InputSplit split : splits) {
            CombineFileSplit csplit = (CombineFileSplit) split;
            wsplits[i++] = new MyHiveInputSplit(csplit);
        }
        
        for (MyHiveInputSplit _wsplit : wsplits) {
            LOG.info(_wsplit.toString());
        }
        LOG.info("wsplit length " + wsplits.length);
        return wsplits;
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public RecordReader<Text, BytesWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
            throws IOException {
        
        MyHiveInputSplit wsplit = (MyHiveInputSplit) split;
        return new CombineFileRecordReader(job, wsplit.getCombineSplit(), reporter, MyHiveCombineRecordReader.class);
    }
    
}
