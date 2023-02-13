package com.mapred.flow2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

// 可以和Mapper类的输出结果一样
public class FlowPartitioner extends Partitioner<Text, FlowBean>  {

    /*
    *
    * 需求：flowcount案例02
      统计规定文档的  总上行流量 ，总下行流量，总流量，并且需要根据手机号得归属地不同省份的写出道
      不同文件中
      以137开头的单独生成一个结果文件
        使用语法：
        1、基本map，reduce
        2、自定义了FlowBean这个序列化得对象

    *
    *
    * */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String phone = text.toString();// 键是电话号码
        int partition; //根据返回类型决定的代表分区号的变量
        if (phone.startsWith("134")){
            //设置好分区为0
            partition = 0;
        }else if (phone.startsWith("135")){
            // 设置分区为1
            partition = 1;
        }else if (phone.startsWith("137")){
            // 设置分区为2
            partition = 2;
        }else if (phone.startsWith("138")){
            //设置分区3
            partition = 3;
        }else if (phone.startsWith("139")){
            //设置分区为4
            partition = 4;
        }else {
            partition = 5;
        }
        return partition;
    }
}
