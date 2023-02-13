package com.作业.dome2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;


public class vehicleDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver","jdbc:mysql://localhost:3306/long?","root","123456");
        Job job = Job.getInstance(conf);

        job.setJarByClass(vehicleDriver.class);
        job.setMapperClass(vehicleMapper.class);
        job.setReducerClass(vehileReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(vehicleBean.class);

        job.setOutputKeyClass(vehicleBean.class);
        job.setOutputValueClass(NullWritable.class);

        Path input = new Path("./data2");
//        Path output = new Path("./output_file/car1");
        FileInputFormat.setInputPaths(job, input);
//        FileOutputFormat.setOutputPath(job, output);


        DBOutputFormat.setOutput(job,"vehicle", "vehicleID","plateNo", "model","type","lineID","driverID");
        job.setOutputFormatClass(DBOutputFormat.class);
        boolean flag = job.waitForCompletion(true);
        System.exit(flag?0:1);

    }
}
