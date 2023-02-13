package com.mysqla.demo2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MysqlReadReduce extends Reducer<Text, IntWritable,MySqlWritable, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for(IntWritable count:values){
            sum+=count.get();
        }
        MySqlWritable outkey=new MySqlWritable();
        outkey.setCount(sum);
        outkey.setWord(key.toString());
        context.write(outkey,NullWritable.get());
    }
}
