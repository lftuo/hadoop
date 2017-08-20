package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;

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

    /**
     * 出事化hdfs
     * @throws IOException
     * @throws URISyntaxException
     * @throws InterruptedException
     */
    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {
        conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.set("dfs.permissions","false");
        fs = FileSystem.get(new URI("hdfs://VM-89-210-ubuntu:9000"),conf,"root");
    }
}
