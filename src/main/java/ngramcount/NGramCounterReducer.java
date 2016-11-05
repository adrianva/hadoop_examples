package ngramcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramCounterReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable outValue = new LongWritable();

    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long count = 0;
        for (LongWritable value: values) {
            count += value.get();
        }
        outValue.set(count);
        context.write(key, outValue);
    }
}
