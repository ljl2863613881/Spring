package com.wordcount.zuoye.shu1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MysqlReadDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //第一步：创建配置文件，并且创建任务 Job类对象，刚刚创建出来，需要进行配置，所以JOB类的对象状态是 DEFINE
        //JOB类静态方法调用，可以不用通过对象去调用方法，而是直接通过类
        //要么抛出 要么处理
        Configuration conf=new Configuration();
        //TODO 添加数据库配置
        DBConfiguration.configureDB(conf,"com.mysql.jdbc.Driver","jdbc:mysql://localhost:3306/test","root","123456");
        Job job=Job.getInstance(conf);
        //todo 根据数据库输入 的各个参数查询出数据，通过 MySqlWritable的readFields读取查询的数据
        // tablename要查询的表  conditions查询条件 orderby 排序  后面接要查询的列【多个】
        DBInputFormat.setInput(job, MySqlWritable.class,"shuju",null,null,"id","goodsName","goodsType","goodsCount","goodsPrice");
        //设置输入输入方式为 数据库输入方式，使用DBInputFormat
        job.setInputFormatClass(DBInputFormat.class);

        job.setJarByClass(MysqlReadDriver.class);
        job.setMapperClass(MysqlReadMapper.class);
        job.setReducerClass(MysqlReadReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path  output=new Path("./MySQLreadoutput"); //不存在
        FileOutputFormat.setOutputPath(job   ,  output);
        boolean flag=job.waitForCompletion(true);
        System.exit(  flag ?   0 : 1  );
    }
}

