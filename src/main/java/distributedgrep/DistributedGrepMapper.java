package distributedgrep;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistributedGrepMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    Text outKey = new Text();
    LongWritable one = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
        String expression = context.getConfiguration().get("grep", "");
        if (value.toString().contains(expression)) {
            outKey.set(value);
            context.write(outKey, one);
        }
    }
}
