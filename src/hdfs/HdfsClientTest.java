package hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class HdfsClientTest {
	FileSystem fs = null;
	@Before
	public void init() throws IOException{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://VM-89-210-ubuntu:9000");
		fs = FileSystem.get(conf);
	}
	
	@Test
	public void testUpload() throws IllegalArgumentException, IOException{
		fs.copyFromLocalFile(new Path("F:/jar°ü/servlet-2-3.jar/servlet.LICENSE.txt"), new Path("/servlet.LICENSE111.txt"));
		fs.close();
	}
}
