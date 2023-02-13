package com.mysqla.Mysqlread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MysqlReadDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver","jdbc:mysql://localhost:3306/test","root","123456");
        Job job=Job.getInstance(conf);
        job.setJarByClass(MysqlReadDriver.class);
        job.setMapperClass(MysqlReadMapper.class);
        job.setReducerClass(MysqlReadReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        DBInputFormat.setInput(job,MysqlWritbale.class,"shuju",null,null,"id","goodsName","goodsType","goodsCount","goodsPrice");
        job.setInputFormatClass(DBInputFormat.class);

        Path output=new Path("./mysqloutput");
        FileOutputFormat.setOutputPath(job,output);
        boolean flag=job.waitForCompletion(true);
        System.exit(flag? 0:1);
    }
}
