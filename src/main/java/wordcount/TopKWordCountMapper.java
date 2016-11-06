package wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Mapper class for top k word count
 */
public class TopKWordCountMapper extends Mapper<LongWritable, Text, LongWritable, TextArrayWritable> {

    private int k = 10;

    private TreeMap<Long, ArrayList<Text>> words = new TreeMap<Long, ArrayList<Text>>(Collections.reverseOrder());
    private static final String SEP = "\\s+";

    @Override
    protected void setup(Mapper<LongWritable, Text, LongWritable, TextArrayWritable>.Context context) throws IOException{
        k = context.getConfiguration().getInt("top_k", k);
    }

    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, LongWritable, TextArrayWritable>.Context context)
            throws IOException, InterruptedException {

        Text word = new Text(value.toString().split(SEP)[0]);
        long count = Long.parseLong(value.toString().split(SEP)[1]);

        ArrayList<Text> keyList = words.get(count);

        if(keyList == null){
            keyList = new ArrayList<Text>();
        }
        keyList.add(word);
        words.put(count, keyList);

        // We only need to keep in memory a treemap os size k
        if(words.size() > k){
            words.remove(words.lastKey());
        }
    }

    /*
        In this cleanup method we simply emit lists of words and their frequencies
        until the sum of the words emitted is greater or equal to K
     */
    @Override
    protected void cleanup(Mapper<LongWritable, Text, LongWritable, TextArrayWritable>.Context context)
            throws IOException, InterruptedException {

        int count = k;
        for (Long key: words.keySet()){
            if(count > 0) {
                ArrayList<Text> listOfWords = new ArrayList<Text>();
                for (Text word: words.get(key)){
                    listOfWords.add(new Text(word));
                }

                LongWritable frequency = new LongWritable(key);
                Text[] listOfWordsTextArray = listOfWords.toArray(new Text[listOfWords.size()]);
                context.write(frequency, new TextArrayWritable(listOfWordsTextArray));
                // Substract the number of words we have emitted
                count -= listOfWords.size();
            }else {
                break;
            }
        }
    }
}
