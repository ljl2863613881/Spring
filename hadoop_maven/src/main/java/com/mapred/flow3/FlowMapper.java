package com.mapred.flow3;



import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1 获取一行数据,转成字符串
        String line = value.toString();

        //2 切割数据
        String[] split = line.split("\t");

        //3 抓取我们需要的数据:手机号,上行流量,下行流量
        String phone = split[1]; //文件中第二个值，所以下标为1
        String up = split[split.length - 2];//倒数第二个
        String down = split[split.length - 3];//倒数第三个

        //4 封装outK outV
        FlowBean mapOutBean = new FlowBean();
        mapOutBean.setUpFlow(Long.parseLong(up));//将String类型转换为Long
        mapOutBean.setDownFlow(Long.parseLong(down));//将String类型转换为Long
        mapOutBean.setSumFlow(Long.parseLong(up) + Long.parseLong(down));
//        mapOutBean.setSumFlow(0L);
//        mapOutBean.setSumFlow();
//        outK.set(phone);
//        outV.setUpFlow(Long.parseLong(up));
//        outV.setDownFlow(Long.parseLong(down));
//        outV.setSumFlow();

        //5 写出outK outV
//        context.write(outK, outV);
        context.write(mapOutBean, new Text(phone));
    }
}