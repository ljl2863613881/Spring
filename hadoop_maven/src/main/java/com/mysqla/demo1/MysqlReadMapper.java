package com.mysqla.demo1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MysqlReadMapper extends Mapper<LongWritable,MySqlWritable, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, MySqlWritable value, Context context) throws IOException, InterruptedException {
        String content=value.toString();
        context.write(new Text(content),NullWritable.get());
    }
}