package com.mysqla.demo2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MysqlReadMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    /*1、Longwritable:表示worder传入KEY的数据类型，默认是一行起始偏移量
    2、Text:表示worder传入VALUE的数据类型，默认是下一行的文本内容
    3、Test:表示自己map方法产生产生的结果数据类型KEY
    4、FLowBean:表示自己map方法产生的结果数据的VALUE类型
     */
    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException, InternalError, InterruptedException {
        //获取一行
        String line = value.toString();
        //切割
        String[] words = line.split(",");
        //输出到mapred
        for (String word:words){
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
