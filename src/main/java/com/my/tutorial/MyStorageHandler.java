package com.my.tutorial;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;


/**
 * @author sabo
 *
 */
@SuppressWarnings("deprecation")
public class MyStorageHandler implements HiveStorageHandler {

    
    private Configuration conf;
    
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return this.conf;
    }

    @SuppressWarnings("rawtypes")
    public Class<? extends InputFormat> getInputFormatClass() {
        return MyHiveInputFormat.class;
    }

    @SuppressWarnings("rawtypes")
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return HiveIgnoreKeyTextOutputFormat.class;
    }

    public Class<? extends SerDe> getSerDeClass() {
        return MyHiveSerDe.class;
    }

    public HiveMetaHook getMetaHook() {
        return null;
    }

    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return null;
    }

    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        // TODO Auto-generated method stub
    }

    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        // TODO Auto-generated method stub
    }

    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        // TODO Auto-generated method stub
    }

    public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
        // TODO Auto-generated method stub
    }

}
