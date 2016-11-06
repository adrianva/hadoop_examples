package wordcount;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Reducer class for top k word count
 */
public class TopKWordCountReducer  extends Reducer<LongWritable, TextArrayWritable, LongWritable, Text> {

    private int k = 10;
    private TreeMap<Long, ArrayList<Text>> words = new TreeMap<Long, ArrayList<Text>>(Collections.reverseOrder());

    @Override
    protected void setup(Reducer<LongWritable, TextArrayWritable, LongWritable, Text>.Context context)
            throws IOException{
        k = context.getConfiguration().getInt("top_k", k);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<TextArrayWritable> values,
                          Reducer<LongWritable, TextArrayWritable, LongWritable, Text>.Context context)
            throws IOException, InterruptedException {

        ArrayList<Text> listOfWords = new ArrayList<Text>();
        for (TextArrayWritable val: values) {
            for (Writable writable: val.get()) {
                Text word = (Text) writable;
                listOfWords.add(word);
            }
        }

        words.put(key.get(), listOfWords);
    }

    @Override
    protected void cleanup(Reducer<LongWritable, TextArrayWritable, LongWritable, Text>.Context context)
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
