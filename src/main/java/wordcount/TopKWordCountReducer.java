package wordcount;

import org.apache.hadoop.io.Writable;
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

    /*
        Basically, in this reduce method we deseialize the TextArrayWritable into a ArrayList<Text>
        and put the result in a treemap in order to keep the list of words for a given frequency
    */
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

    /*
        In the cleanuo method we simply emit the firs K words of the treemap
        (if there is a tie the result is printed in order of appearance
     */
    @Override
    protected void cleanup(Reducer<LongWritable, TextArrayWritable, LongWritable, Text>.Context context)
            throws IOException, InterruptedException {

        int count = k;
        boolean keep_printing = true;
        for (Long key: words.keySet()){
            if(keep_printing) {
                LongWritable frequency = new LongWritable(key);
                for(Text word: words.get(key)) {
                    if(count <= 0) {
                        keep_printing = false;
                        break;
                    }
                    context.write(frequency, new Text(word.toString()));
                    count--;
                }
            }else{
                break;
            }
        }
    }
}
