package com.wordcount.zuoye.shu2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//<Text, NullWritable>  读取的数据类型
// Reduce Task这个对象会调用OutputFormat对象的getRecordWriter方法，默认我们用的是TextOutputFormat，然后它对应的RecordWriter就是LineRecord
public class LogOutputFormat extends FileOutputFormat<Text, NullWritable> {
    /**
     * 从Rducer操作完了之后通过getRecordWriter处理reduce输出的结果
     * @param taskAttemptContext     上下文对象
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //创建一个自定义的RecordWriter返回
        LogRecordWriter logRecordWriter = new LogRecordWriter(taskAttemptContext);
        return logRecordWriter;

    }

}
