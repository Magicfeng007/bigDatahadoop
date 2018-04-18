package com.magic.hadoop.app.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author: Magicfeng007
 * @Description:
 * @Date: Created in 2018-04-18---下午8:44
 */
public class MaxTemperatureReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> iterator = values.iterator();
        int maxTemp = -99999;
        while (iterator.hasNext()){
            if(iterator.next().get() > maxTemp){
                maxTemp = iterator.next().get();
            }
        }
        IntWritable maxTempe = new IntWritable();
        maxTempe.set(maxTemp);
        context.write(key,maxTempe);
    }
}
