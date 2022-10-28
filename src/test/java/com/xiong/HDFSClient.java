package com.xiong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HDFSClient {
    //分布式文件系统
    FileSystem fileSystem;

    private static Path getAbsPath(String path) {
        String classpath = ClassLoader.getSystemResource("").getPath();
        return new Path(classpath + path);
    }


    @Before
    public void init() {
        Configuration configuration = new Configuration();
        // 默认以程序执行的用户名, 即administration进行访问. 这里设置以root进行访问
        System.setProperty("HADOOP_USER_NAME", "root");
        String userName = "root";

        //获取客户端对象
        try {
            URI uri = new URI("hdfs://hadoop001:8020");
            fileSystem = FileSystem.get(uri, configuration, userName);
        } catch (URISyntaxException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void mkdirTest() {
        String filepathStr = "/sanguo";
        Path path = new Path(filepathStr);
        try {
            if (!fileSystem.exists(path)) {
                fileSystem.mkdirs(path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void uploadTest() {
        //源文件的本地路径, 测试环境下需要手动将资源文件移动到test-classes目录下
        Path[] srcFilePaths = new Path[]{getAbsPath("sunquan.txt"), getAbsPath("caocao.txt"), getAbsPath("liubei.txt")};
        //上传的目标地址
        Path dstFilePath = new Path("/sanguo");
        try {
            fileSystem.copyFromLocalFile(false, true, srcFilePaths, dstFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void downloadTest() {
        Path src = new Path("");
        Path dst = new Path("");
        try {
            fileSystem.copyToLocalFile(false, src, dst, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteTest() {
        Path path = new Path("");
        try {
            fileSystem.delete(path, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void moveTest() {
        Path src = new Path("");
        Path dst = new Path("");
        try {
            fileSystem.rename(src, dst);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void fileDetailTest() {
        Path path = new Path("/");
        try {
            RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, true);
            while (files.hasNext()) {
                LocatedFileStatus fileStatus = files.next();
                System.out.println("fileStatus.getPath() = " + fileStatus.getPath());
                System.out.println("fileStatus.isFile() = " + fileStatus.isFile());
                System.out.println("fileStatus.isDirectory() = " + fileStatus.isDirectory());
                System.out.println("fileStatus.getGroup() = " + fileStatus.getGroup());
                System.out.println("fileStatus.getBlockLocations() = " + Arrays.toString(fileStatus.getBlockLocations()));
                System.out.println("fileStatus.getBlockSize() = " + fileStatus.getBlockSize());
                System.out.println("*****************************************************************************");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @After
    public void close() {
        //关闭资源
        try {
            if (fileSystem != null) {
                fileSystem.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
