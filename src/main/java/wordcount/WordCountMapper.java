package wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    Text outKey = new Text();
    LongWritable one = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
    throws IOException, InterruptedException {
        String[] splits = value.toString().trim().split("\\s+");
        for (String split: splits) {
            outKey.set(split);
            context.write(outKey, one);
        }
    }
}
