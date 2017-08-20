package hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 用流的方式操作hdfs上的文件
 * 可以实现制定偏移范围的数据
 */
public class HdfsStreamAccess {
    static FileSystem fs = null;
    Configuration conf = null;

    private static Logger logger = LoggerFactory.getLogger(HdfsStreamAccess.class);

    /**
     * 出事化hdfs
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {
        PropertyConfigurator.configure("log4j.properties");

        conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.set("dfs.permissions","false");
        fs = FileSystem.get(new URI("hdfs://VM-89-210-ubuntu:9000"),conf,"root");
    }

    @Test
    public void testUpload() throws IOException {
        FSDataOutputStream fsDataOutputStream = fs.create(new Path("/streamupload.txt"));
        FileInputStream fileInputStream = new FileInputStream("F:/test/streamupload.txt");
        IOUtils.copy(fileInputStream,fsDataOutputStream);
    }

    @Test
    public void testDownload() throws IOException {
        FSDataInputStream fsDataInputStream = fs.open(new Path("/streamupload.txt"));
        FileOutputStream fileOutputStream = new FileOutputStream("C:/Users/tuotuo/Desktop/streamupload");
        IOUtils.copy(fsDataInputStream,fileOutputStream);
    }

    /**
     * 按字节读取
     * @throws IOException
     */
    @Test
    public void testRandomAccess() throws IOException {
        FSDataInputStream inputStream = fs.open(new Path("/streamupload.txt"));
        inputStream.seek(20);
        FileOutputStream outputStream = new FileOutputStream("C:/Users/tuotuo/Desktop/ndomread.txt");
        IOUtils.copy(inputStream,outputStream);
    }
}
