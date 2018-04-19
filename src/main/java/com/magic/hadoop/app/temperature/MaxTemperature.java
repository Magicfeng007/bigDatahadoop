package com.magic.hadoop.app.temperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author: Magicfeng007
 * @Description: 获取NCDC每年最高的温度
 * 运行时需要使用命令：
 * hadoop jar bigData-hadoop-1.0-SNAPSHOT-without.jar com.magic.hadoop.app.temperature.MaxTemperature ./data/ output11
 * 类型必须为全包名，且需要使用jar指定jar包，输入文件目录必须为./data/，而不能为./data/*，输出目录必须不存在
 * @Date: Created in 2018-04-15---下午6:17
 */
public class MaxTemperature {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            //Job对象指定作业执行规范，可以用他来控制整个作业的运行
            Job job = Job.getInstance(conf,"MaxTemperature");


            //必须指定，否则报错Error: java.lang.RuntimeException: java.lang.ClassNotFoundException:
            // Class com.magic.hadoop.app.temperature.MaxTemperatureMapper not found
            job.setJarByClass(MaxTemperature.class);

            job.setMapperClass(MaxTemperatureMapper.class);
            job.setReducerClass(MaxTemperatureReducer.class);

            //job.setCombinerClass(MaxTemperatureReducer.class);可以不设置，一般情况不要和setReducerClass同时用，否则可能导致reducer运行两次出问题


            //经测试发现，即使mapper产生出与reducer相同的类型时，setOutputKeyClass与
            // setOutputValueClass也需要设置才能正常运行
            //否则会报错类型不匹配，如：
            //Error: java.io.IOException: Type mismatch in key from map:
            // expected org.apache.hadoop.io.LongWritable,
            // received org.apache.hadoop.io.IntWritable
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(IntWritable.class);

            //文件输入路径，可以是目录，如：./data/，这样会输入该目录下所有的文件
            //也可以是单个文件的具体路径，如：./data/1901  (1901是文件名称)
            FileInputFormat.addInputPath(job,new Path(args[0]));

            //结果输入路径，该目录必须不存在，否则报错，该输出目录已存在：
            //org.apache.hadoop.mapred.FileAlreadyExistsException:
            // Output directory hdfs://magic:8082/user/magic/output11 already exists
            FileOutputFormat.setOutputPath(job,new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }catch (IOException e){
            e.printStackTrace();
        }catch (InterruptedException e){
            e.printStackTrace();
        }catch(ClassNotFoundException e){
            e.printStackTrace();
        }



    }
}
