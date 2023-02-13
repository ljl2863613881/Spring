package com.mysqla.Mysqlread;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MysqlReadMapper extends Mapper<LongWritable,MysqlWritbale, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, MysqlWritbale value, Context context) throws IOException, InterruptedException {
        context.write(new Text(value.toString()),NullWritable.get());
    }
}
