package com.magic.hadoop.app.fs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

/**
 * @Author: Magicfeng007
 * @Description: 从hadoop hdfs 读取文件并输出
 * 运行时需要使用命令：
 * hadoop jar bigData-hadoop-1.0-SNAPSHOT.jar com.magic.hadoop.app.fs.URLcat hdfs://magic:8082/user/magic/core-site.xml
 * @Date: Created in 2018-04-22---下午4:26
 */
public class URLcat {
    static {
        //通过注册URLStreamHandlerFactory的实现类：FsUrlStreamHandlerFactory
        //让Java程序能够识别Hadoop的hdfs URL方案
        //每个java虚拟机只能调用一次这个方法，因此通常在静态方法中调用
        //这个限制意味着如果程序的其他组件（如不受你控制的第三方组件）已经声明一个URLStreamHandlerFactory
        //的实例，你将无法使用这种方法从Hadoop中读取数据
        //但可通过Apache Commons Sandbox，就是commons-jnet解决此限制
        //它基本原理就是使用java reflect技术，强行改变URL中的私有成员变量factory(类型为URLStreamHandlerFactory)
        // 来保setURLStreamHandlerFactory能被成功调用,并且不破坏原有的factory
        //具体可参考：https://blog.csdn.net/10km/article/details/54343973

        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }
    public static void main(String[] args) throws Exception{
        InputStream in = null;
        try{
            //args[0]:  hdfs://host/path
            in = new URL(args[0]).openStream();
            //在输入和输出流之间复制数据，4096为缓冲区的大小，false为设置复制结束后不关闭数据流[这样System.out不必关闭输入流]，在finally中手工关闭
            IOUtils.copyBytes(in,System.out,4096,false);
        }finally {
            IOUtils.closeStream(in);
        }
    }
}
