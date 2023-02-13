package com.作业.dome1.写入;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MysqlReadReduce extends Reducer<Text, MySqlWritable,MySqlWritable, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<MySqlWritable> values, Context context) throws IOException, InterruptedException {
        for(MySqlWritable mysqlWritable:values){
            context.write(mysqlWritable,NullWritable.get());

        }
    }
}
