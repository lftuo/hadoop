package hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

public class HdfsClientTest {
	static FileSystem fs = null;
	Configuration conf = null;

	/**
	 * 初始化hdfs
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


	/**
	 * 测试hdfs文件写入
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testUpload() throws IllegalArgumentException, IOException{

		fs.copyFromLocalFile(new Path("F:/test/test.txt"), new Path("/test.txt"));
		fs.close();
	}

	/**
	 * 测试hdfs文件下载
	 * @throws IOException
	 */
	@Test
	public void download() throws IOException {
		fs.copyToLocalFile(new Path("/count.txt"),new Path("F:\\test\\count.txt"));
		fs.close();
	}

	/**
	 * 打印hdfs-default.xml
	 */
	@Test
	public void testConf(){
		Iterator<Map.Entry<String,String>> iterator = conf.iterator();
		while (iterator.hasNext()){
			Map.Entry<String,String> entry = iterator.next();
			System.out.println(entry.getKey()+":" + entry.getValue());
		}
	}

	/**
	 * 测试递归创建目录
	 * @throws IOException
	 */
	@Test
	public void testMkdirs() throws IOException {
		boolean hasCreated = fs.mkdirs(new Path("/testmkdir/aaa/input"));
		System.out.println(hasCreated);
	}

	/**
	 * 递归删除目录下文件
	 * @throws IOException
	 */
	@Test
	public void testDelete() throws IOException {
		boolean flag = fs.delete(new Path("/testmkdir/aaa/input"),true);
		System.out.println(flag);
	}

	/**
	 * 递归显示目录下的子文件
	 * @throws IOException
	 */
	@Test
	public void testLs() throws IOException {
		RemoteIterator<LocatedFileStatus> statusRemoteIterator = fs.listFiles(new Path("/"),true);
		while (statusRemoteIterator.hasNext()){
			LocatedFileStatus status = statusRemoteIterator.next();
			System.out.println("FileName:" + status.getPath());
			System.out.println("BlockSize:" + status.getBlockSize());
			System.out.println("Owner:"+status.getOwner());
			System.out.println("Replication:"+status.getReplication());
			System.out.println("Permission:"+status.getPermission());
			System.out.println("--------------------------------");
		}
	}


	/**
	 * 文件属性判断
	 * @throws IOException
	 */
	@Test
	public void testLs2() throws IOException {
		FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
		for (FileStatus status : fileStatuses) {
			System.out.println("name : "+status.getPath());
			System.out.println(status.isDirectory()?"directory":"file");
		}
	}
}
