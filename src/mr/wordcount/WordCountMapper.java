package mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * map程序
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

    /**
     * maptask会对每一行输入数据调用一次如下自定义的map程序，然后maptask对每一行调用结果汇总发回reduce
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            // 将单词作为key，将1作为value，以便于后续数据分发，可以根据单词分发，一遍相同的单词会到相同的reduce task
            context.write(new Text(word),new IntWritable(1));
        }

    }
}
