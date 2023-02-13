package com.wordcount.zuoye.shu1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MysqlReadReduce extends Reducer<Text, NullWritable, Text,NullWritable> {
    private MySqlWritable outV = new MySqlWritable();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//        String gtype;
//        double jiage;
//        int shuliang;
//        double p=0;
//        gtype=outV.getGoodsType();
//
//
//        if (gtype.equals(outV.getGoodsType())){
//            for (DoubleWritable conut:values){
//                jiage=outV.getGoodsPrice();
//                shuliang=outV.getGoodsCount();
//                p=jiage/shuliang;
//            }
//
//        }
//        context.write(key,new DoubleWritable(p));
        context.write(key,NullWritable.get());
    }
}

