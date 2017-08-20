package mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 相当于一个yarn集群的客户端
 * 需要再此封装mr程序的封装运行参数，制定jar包，最后交给yarn
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        conf.set("dfs.permissions","false");

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCountDriver.class);

        // 指定本业务job使用的mapper业务类
        job.setMapperClass(WordCountMapper.class);
        // 指定本业务job使用的reducer业务类
        job.setReducerClass(WordCountReducer.class);

        // 指定mapper的输出类型（有第三方序列化接口）
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 指定最终输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 指定job输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path("/wordcount/input"));
        // 指定job输出结果所在目录
        FileOutputFormat.setOutputPath(job,new Path("/wordcount/output"));


        // job中配置的相关参数，以及job所用到的Java类所在的jar包，交个yarn运行
        // job.submit();/*非阻塞*/
        /*阻塞式的*/
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
