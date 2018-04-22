package com.magic.hadoop.app.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @Author: Magicfeng007
 * @Description: 追加文件到hdfs中的文件末尾
 * 执行命令：
 * hadoop jar bigData-hadoop-1.0-SNAPSHOT.jar com.magic.hadoop.app.fs.FileAppend /home/magic/IdeaProjects/bigDataHadoop/src/main/java/com/magic/hadoop/app/fs/FileAppend.java hdfs://magic:8082/user/magic/core-site.xml
 * @Date: Created in 2018-04-22---下午7:48
 */
public class FileAppend {
    public static void main(String[] args) throws Exception{
        String localFile = args[0];
        String dstFile = args[1];
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        //追加文件内容到末尾
        OutputStream out = fileSystem.append(new Path(dstFile),4096,()->{System.out.print(".");});
        InputStream in = new FileInputStream(localFile);
        IOUtils.copyBytes(in,out,4096,true);
    }
}
