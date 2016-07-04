package com.my.tutorial;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

public class MyHiveInputSplit extends HiveInputSplit {
    
    private CombineFileSplit combineSplit;

    @Override
    public void readFields(DataInput in) throws IOException {
        if (combineSplit == null) {
            combineSplit = new CombineFileSplit();
        }
        combineSplit.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        combineSplit.write(out);
    }
    
    public MyHiveInputSplit() {}

    public MyHiveInputSplit(CombineFileSplit combineSplit) {
        this.setCombineSplit(combineSplit);
    }


    public CombineFileSplit getCombineSplit() {
        return combineSplit;
    }

    public void setCombineSplit(CombineFileSplit combineSplit) {
        this.combineSplit = combineSplit;
    }
    
    @Override
    public String[] getLocations() throws IOException {
        return combineSplit.getLocations();
    }
    
    @Override
    public long getLength() {
        return combineSplit.getLength();
    }
    
    @Override
    public Path getPath() {
        return combineSplit.getPaths()[0];
    }
    
    @Override
    public String toString() {
        return combineSplit.toString();
    }
}
