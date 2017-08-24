package mr.flowcount;

import mr.flowcount.bean.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 流量求和
 */
public class FlowCount{
    static class FlowCountMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString() != null && value.toString().trim().length() > 0){
                String line = value.toString();
                String[] fields = line.split("\t");
                String phoneNumber = fields[1];
                String upFlow = fields[fields.length-3];
                String dFlow = fields[fields.length-2];
                context.write(new Text(phoneNumber),new FlowBean(Long.parseLong(upFlow),Long.parseLong(dFlow)));
            }
        }
    }

    static class FlowCountReducer extends Reducer<Text,FlowBean,Text,FlowBean>{
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

            long sum_upFlow = 0;
            long sum_downFlow = 0;
            for (FlowBean flowBean : values){
                sum_upFlow += flowBean.getUpFlow();
                sum_downFlow += flowBean.getdFlow();
            }
            FlowBean resultBean = new FlowBean(sum_upFlow,sum_downFlow);
            context.write(key,resultBean);
        }

    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        conf.set("dfs.permissions","false");

        Job job = Job.getInstance(conf);
        job.setJarByClass(FlowCount.class);

        // 指定本业务job使用的mapper业务类
        job.setMapperClass(FlowCountMapper.class);
        // 指定本业务job使用的reducer业务类
        job.setReducerClass(FlowCountReducer.class);

        // 指定mapper的输出类型（有第三方序列化接口）
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 指定最终输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 指定job输入原始文件所在目录
        FileInputFormat.setInputPaths(job,new Path("hdfs://VM-89-210-ubuntu:9000/flowcount/input"));
        // 指定job输出结果所在目录
        FileOutputFormat.setOutputPath(job,new Path("hdfs://VM-89-210-ubuntu:9000/flowcount/output"));


        // job中配置的相关参数，以及job所用到的Java类所在的jar包，交个yarn运行
        // job.submit();/*非阻塞*/
        /*阻塞式的*/
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }

}
