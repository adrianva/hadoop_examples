package distributedgrep;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistributedGrepMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    Text outKey = new Text();
    LongWritable one = new LongWritable(1);
    String expression;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException{
        expression = context.getConfiguration().get("grep", "").toLowerCase();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
        if (value.toString().toLowerCase().contains(expression)) {
            outKey.set(value);
            context.write(outKey, one);
        }
    }
}
