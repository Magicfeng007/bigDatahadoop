package com.magic.hadoop.app.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @Author: Magicfeng007
 * @Description: 从本地复制文件到hadoop 中
 *使用命令行执行：
 * hadoop jar bigData-hadoop-1.0-SNAPSHOT.jar com.magic.hadoop.app.fs.FileCopyWithProgress ./bigData-hadoop-1.0-SNAPSHOT.jar hdfs://magic:8082/user/magic/jarfile/bigData-hadoop-1.0-SNAPSHOT.jar
 * @Date: Created in 2018-04-22---下午6:00
 */
public class FileCopyWithProgress {

    public static void main(String[] args)throws Exception {
        String localFile = args[0];
        String dstFile = args[1];
        InputStream in = new BufferedInputStream(new FileInputStream(localFile));
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);

        //使用匿名内部类实现
        /*fileSystem.create(new Path(dstFile), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });*/


        //使用jdk8中的lambda表达式实现
        //如果文件父目录不存在则会自动创建
        OutputStream outputStream = fileSystem.create(new Path(dstFile), ()->{
            System.out.print(".");
        });
        IOUtils.copyBytes(in,outputStream,4096,true);
    }
}
