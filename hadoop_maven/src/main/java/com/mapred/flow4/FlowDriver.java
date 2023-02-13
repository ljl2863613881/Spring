package com.mapred.flow4;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //1 获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2 关联本Driver类
        job.setJarByClass(FlowDriver.class);

        //3 关联Mapper和Reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

//4 设置Map端输出KV类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

//5 设置程序最终输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

//6 设置程序的输入输出路径
        FileInputFormat.setInputPaths(job, new Path("./D:\\java\\项目\\Spring\\hadoop_maven/inputflow"));
        FileOutputFormat.setOutputPath(job, new Path("/D:\\java\\项目\\Spring\\hadoop_maven\\flowoutput"));

        //根据分区的数量，设定相同数量的ReduceTak数量
        job.setPartitionerClass(FlowPartitioner.class);
        job.setNumReduceTasks(6);
        /*如果 reduceTask 的数量> getPartition 的结果数，则会多产生几个空的输出文件 part-r-000xx；
         * 如果 1<reduceTask 的数量<getPartition 的结果数，则有一部分分区数据无处安放，会 Exception；
         *如果 reduceTask 的数量=1，则不管 mapTask 端输出多少个分区文件，最终结果都交给这一个 reduceTask，最终也就只会产生一个结果文件 part-r-00000；
         * */

//7 提交Job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}