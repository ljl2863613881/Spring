package com.mysqla.demo2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class MysqlReadDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        //第一步：创建配置文件，并且创建任务 Job类对象，刚刚创建出来，需要进行配置，所以JOB类的对象状态是 DEFINE
        //JOB类静态方法调用，可以不用通过对象去调用方法，而是直接通过类
        //要么抛出 要么处理
        Configuration conf = new Configuration();
        //TODO 添加数据库配置
        DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver","jdbc:mysql://localhost:3306/test","root","123456");
        Job job=Job.getInstance(conf);

//        job.setJarByClass(MysqlReadDriver.class);
//        job.setMapperClass(MysqlReadMapper.class);
//        job.setReducerClass(MysqlReadReduce.class);
//
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(NullWritable.class);
//
//        job.setOutputKeyClass(MySqlWritable.class);
//        job.setOutputValueClass(NullWritable.class);

        job.setJarByClass(MysqlReadDriver.class);
        job.setMapperClass(MysqlReadMapper.class);

        job.setReducerClass(MysqlReadReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(MySqlWritable.class);
        job.setOutputValueClass(NullWritable.class);

        Path  input=new Path("./D:\\java\\项目\\Spring\\hadoop_maven/data1");//一定要真是存在
        FileInputFormat.addInputPath(job   ,  input);

        //输出的数据是放入数据库那个
        //TODO：设置数据写入到那张表 的那些字段  tablename 表 fieldName 字段
        DBOutputFormat.setOutput(job,"test_word","word","count");
        //todo:设置输出的类型为DBOutputFormat
        job.setOutputFormatClass(DBOutputFormat.class);

        boolean fiag = job.waitForCompletion(true);
        System.exit(fiag?0:1);
    }
}
