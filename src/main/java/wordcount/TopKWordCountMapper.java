package wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.TreeMap;

/**
 * Created by Nacho on 05/11/2016.
 */
public class TopKWordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private int k = 10;

    private TreeMap<Long, Text> words = new TreeMap<Long, Text>();
    private LongWritable outValue = new LongWritable();
    private static final String SEP = "\\s+";


    @Override
    protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException{
        k = context.getConfiguration().getInt("topwords.k", k);
    }

    /*@Override
    protected void cleanup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException{



    }*/

    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        Text outKey = new Text(value.toString().split(SEP)[0]);
        long count = Long.parseLong(value.toString().split(SEP)[1]);
        words.put(count, outKey);
        if(words.size()>k){
            words.remove(words.firstKey());
        }
    }
}