package com.my.tutorial;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.my.tutorial.MyProtoBuf.CommonDump;


/**
 * @author sabo
 * Use MapReduce to test my hive input format 
 */
public class App extends Configured implements Tool {

    
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new App(), args));
    }
    
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        
        if (args.length < 2) {
            System.out.println("Usage: hadoop jar <jarname.jar> <inpath> <outpath>");
            System.exit(1);
        }
        Configuration conf = getConf();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        
        FileSystem fs = FileSystem.get(conf);
        fs.delete(output, true);
        
        JobConf job = new JobConf(conf);
        job.setJobName("My HiveInputFormat Test");
        job.setJarByClass(App.class);

        job.setInputFormat(MyHiveInputFormat.class);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        
        //指定Mapper 、 Mapper的输入输出类型
        
        job.setMapperClass(baseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        
        //指定Reducer 、 Reducer的输入输出类型
        job.setNumReduceTasks(0);
        
        try {
            JobClient.runJob(job); //run
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
        return 0;
    }
    
    
    public static class baseMapper extends MapReduceBase implements Mapper<Text, BytesWritable, Text, Text> {

        public void map(Text key, BytesWritable value, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            String buffer = new String();
            byte[] body = value.copyBytes();
            CommonDump commonDump = CommonDump.parseFrom(body);
            buffer = commonDump.getTableId() + "\t" + commonDump.getRowKey().toStringUtf8() + "\t" + commonDump.getValue().toStringUtf8().replaceAll("\n|\r|\t", "");
            output.collect(key, new Text(buffer));
        }
    }
}
