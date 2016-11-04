package wordcount;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable outValue = new LongWritable();

    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text,
            LongWritable>.Context context) throws IOException, InterruptedException {

        long count = 0;
        for (LongWritable value: values) {
            count += value.get();
        }
        outValue.set(count);
        context.write(key, outValue);
    }
}
