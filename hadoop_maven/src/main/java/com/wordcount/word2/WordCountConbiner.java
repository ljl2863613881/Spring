package com.wordcount.word2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import javax.xml.soap.Text;
import java.io.IOException;

/*
* 第一步：父类是reduce==》编写Conbiner类的时候，就照着Reduce
* 第二步：Conbiner数据输入路径，map的输出
* 第三步：Conbiner数据输出路径，reduce输入==》map的输出
*
*
*
*
* */
public class WordCountConbiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int sum = 0;
        for(IntWritable count:values){
            sum+=count.get(); // IntWritable等序列化对象，获取对象内容get()， 设置对象的值的方法set
        }
        context.write(key,new IntWritable(sum));
    }
}
