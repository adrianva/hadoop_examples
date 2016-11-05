package ngramcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NGramCounterMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    Text outKey = new Text();
    LongWritable one = new LongWritable(1);
    Integer ngram_length;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException{
        ngram_length = context.getConfiguration().getInt("n", 1);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
        String[] splits = value.toString().trim().split("\\s+");

        String ngram;
        for (int i = 0; i <= splits.length - ngram_length; i++) {
            ngram = "";
            int j = 1;
            int position = i;
            // Each iteration we must obtain "ngram_length" elements from the split
            while (j <= ngram_length) {
                if (position == i) // first element of the ngram
                    ngram += splits[position];
                else
                    ngram += " " + splits[position];

                position++;
                j++;
            }
            outKey.set(ngram);
            context.write(outKey, one);
        }
    }
}
