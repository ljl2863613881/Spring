package com.wordcount.word2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        System.out.println("读取的配置文件"+conf.toString());
        Job job = Job.getInstance(conf);

        // 2 关联本Driver程序的jar
        job.setJarByClass(WordCountDriver.class);

        // 3 关联Mapper和Reducer的jar
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);

        // 4 设置Mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        Path inputPath = new Path("./data");
        Path outputPath = new Path("./output2");
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 7 提交job  提交任务到集群中，并等待集群作业完成
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
         /*1）此过程检查Job状态后，状态OK则提交作业submit()
         2）提交作业先，需要建立连接connect(),此连接过程会创建Cluster对象，并生成cluster的引用
         3）Job类持有Cluster的引用，而Cluster持用ResourceManager进程的引用，Cluster也持有RPC代理对象client
         说明：客户端持有服务端的引用，这样就可以建立RPC通信
         4）Cluster的构造方法 initialize(jobTrackAddr, conf);会创建如下对象 ClientProtocol clientProtocol，而这一个对象就是一个接口，最后再将这一个代理对象赋值给 client = clientProtocol;  而client 就是Cluster的成员变量
         5）上述就是Job建立连接的过程，完成连接后需要得到一个提交器，Job创建一个提交器JobSubmitter submitter
         6）通过提交器将Job，cluster传入submitter.submitJobInternal(Job.this, cluster);
         7）submitter 检查输出目录是否有异常，接着得到一个存储Jar包路径jobStagingArea ，再得到一个JobID，JobID是通过submitter里的RPC引用得到，实际JobID是在服务端实现
         8）submitter提交器将提交jar地址是通过jobStagingArea和JobID拼接而成
         9）submitter提交器copyAndConfigureFiles接口将Jar包和配置信息提交到hdfs里，默认向hdfs写10份（也可以通过配置mapreduce.client.submit.file.replication，当然提交的份数也可以通过读取配置文件获得mapreduce.client.submit.file.replication（mapred-default.xml 682行））
        10）submitter提交器通过copyAndConfigureFiles拷贝Jar信息到hdfs
        11）submitter提交器通过服务端代理对象submitClient.submitJob提交Job信息，服务端最终将信息提交给ResourceManager*/


