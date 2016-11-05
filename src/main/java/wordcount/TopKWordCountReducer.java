package wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Reducer class for top k word count
 */
public class TopKWordCountReducer  extends Reducer<LongWritable, List<Text>, LongWritable, Text> {

    private int k = 10;

    private TreeMap<Long, List<Text>> words = new TreeMap<Long, List<Text>>(Collections.reverseOrder());

    @Override
    protected void setup(Reducer<LongWritable, List<Text>, LongWritable, Text>.Context context)
            throws IOException{
        k = context.getConfiguration().getInt("topwords.k", k);
    }
    @Override
    protected void reduce(LongWritable key, Iterable<List<Text>> values,
                          Reducer<LongWritable, List<Text>, LongWritable, Text>.Context context)
            throws IOException, InterruptedException {


        List<Text> emptyList = new ArrayList<Text>();
        for (List<Text> value : values) {
            emptyList.addAll(value);
        }
        words.put(key.get(), emptyList);
    }

    @Override
    protected void cleanup(Reducer<LongWritable, List<Text>, LongWritable, Text>.Context context)
            throws IOException, InterruptedException {

        int count = k;
        for (Long key: words.keySet()){
            if(count > 0) {
                LongWritable frequency = new LongWritable(key);
                String total = "";
                for(Text word: words.get(key)){
                    total = total + ", " + word.toString();
                }
                context.write(frequency, new Text(total));
                count -= key;
            }else{
                break;
            }
        }


    }
}
