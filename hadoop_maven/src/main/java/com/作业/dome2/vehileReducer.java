package com.作业.dome2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// reduce 输出类型  因为数据写入到Mysql，所以使用的是自定义序列类MysqlWritabe  ，因为键Mysqlwride对象
// 包含所以输出的全部属性
public class vehileReducer extends Reducer<Text, vehicleBean, vehicleBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<vehicleBean> values, Context context) throws IOException, InterruptedException {

        for(vehicleBean mysqlWritable:values){
            context.write(mysqlWritable,NullWritable.get());

        }
    }
}
