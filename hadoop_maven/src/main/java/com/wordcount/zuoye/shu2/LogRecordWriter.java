package com.wordcount.zuoye.shu2;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

//RecordWriter:其实主要就是负责将task的key/value结果写入内存或者磁盘
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
//    然后每次reduce方法执行完毕，都会调用context.write(key，value)
//    这时候LineRecordWriter就会把key/value写入输出文件


    private FSDataOutputStream baiduOut;
    private FSDataOutputStream googleOut;
    private FSDataOutputStream otherOut;
    public LogRecordWriter(TaskAttemptContext job) {
        try {
            //获取文件系统对象
            FileSystem fs = FileSystem.get(job.getConfiguration());
            //用文件系统对象创建两个输出流对应不同的目录
            baiduOut = fs.create(new Path("./logoutput/baidu.log"));
            googleOut = fs.create(new Path("./logoutput/google.log"));
            otherOut = fs.create(new Path("./logoutput/other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 写入键值对输入到文件中
     * @param text
     * @param nullWritable
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String log = text.toString();
        //根据一行的log数据是否包含baidu,判断两条输出流输出的内容
        if (log.contains("baidu")) {
            baiduOut.writeBytes(log + "\n");
        } else if(log.contains("google")){
            googleOut.writeBytes(log + "\n");
        } else {
            otherOut.writeBytes(log + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //关流
        IOUtils.closeStream(baiduOut);
        IOUtils.closeStream(googleOut);
        IOUtils.closeStream(otherOut);
    }
}


       /* 三 常见的RecordWriter

        3.1 DBRecordWriter: 将reduce结果写入sql表中

        3.2 LineRecordWriter: 将key/value写入输出文件一行中

        3.3 NewDirectOutputCollector: 它默认调用的也是LineRecordWriter，输出结果写入输出文件

        3.4 NewOutputCollector: 将key/value写入内存，内存满了写入磁盘，一般情况在Map阶段 */
