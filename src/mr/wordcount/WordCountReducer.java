package mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    /**
     * 一个单词调用一次累加，多个单词统计结果交个reduce task，reduce task生成结果写到hdfs
     * <A,1><A,1><A,1><A,1><A,1><A,1><A,1><A,1>
     * <B,1><B,1><B,1><B,1><B,1><B,1>
     * <C,1><C,1><C,1><C,1><C,1><C,1><C,1><C,1><C,1>
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        context.write(key,new IntWritable(count));
    }
}
