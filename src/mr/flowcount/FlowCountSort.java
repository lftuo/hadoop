package mr.flowcount;

import mr.flowcount.bean.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountSort {

    FlowBean flowBean = new FlowBean();

    static class FlowCountSortMapper extends Mapper<LongWritable,Text,FlowBean,Text>{
        FlowBean flowBean = new FlowBean();
        Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 拿到统计结果的数据每一行，已经是各手机的流量汇总
            String line = value.toString();
            String[] fields = line.split("\t");
            String phnoeNum = fields[0];

            long upFlow = Long.parseLong(fields[1]);
            long downFlow = Long.parseLong(fields[2]);

            flowBean.setFlow(upFlow,downFlow);
            v.set(phnoeNum);
            context.write(flowBean,v);
        }
    }

    static class FlowCountSortReducer extends Reducer<FlowBean,Text,Text,FlowBean>{
        @Override
        protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Iterable<Text> values中只有一条数据，对象作为key，反序列化后不会相同
            context.write(values.iterator().next(),key);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        conf.set("dfs.permissions","false");

        Job job = Job.getInstance(conf);
        job.setJarByClass(FlowCountSort.class);

        // 指定本业务job使用的mapper业务类
        job.setMapperClass(FlowCountSortMapper.class);
        // 指定本业务job使用的reducer业务类
        job.setReducerClass(FlowCountSortReducer.class);

        // 指定mapper的输出类型（有第三方序列化接口）
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 指定最终输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        Path outpath = new Path("hdfs://VM-89-210-ubuntu:9000/flowcount/outsort");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outpath)){
            fs.delete(outpath,true);
        }

        // 指定job输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path("hdfs://VM-89-210-ubuntu:9000/flowcount/output"));
        // 指定job输出结果所在目录
        FileOutputFormat.setOutputPath(job,outpath);


        // job中配置的相关参数，以及job所用到的Java类所在的jar包，交个yarn运行
        // job.submit();/*非阻塞*/
        /*阻塞式的*/
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }

}
