package wordcount;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Mapper class to count the top k words
 */
public class TopKWordCountMapper extends Mapper<LongWritable, Text, LongWritable, List<Text>> {

    private int k = 10;

    private TreeMap<Long, List<Text>> words = new TreeMap<Long, List<Text>>(Collections.reverseOrder());
    private static final String SEP = "\\s+";
    private List<Text> emptyList = new ArrayList<Text>();

    @Override
    protected void setup(Mapper<LongWritable, Text, LongWritable, List<Text>>.Context context) throws IOException{
        k = context.getConfiguration().getInt("topwords.k", k);
    }

    @Override
    protected void map(LongWritable key, Text value,
                       Mapper<LongWritable, Text, LongWritable, List<Text>>.Context context)
            throws IOException, InterruptedException {

        Text outKey = new Text(value.toString().split(SEP)[0]);
        long count = Long.parseLong(value.toString().split(SEP)[1]);
        List<Text> keyList = words.get(count);

        if(keyList == null){
            keyList=emptyList;
        }
        keyList.add(outKey);

        words.put(count, keyList);
        if(words.size() > k){
            words.remove(words.firstKey());
        }
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, LongWritable, List<Text>>.Context context)
            throws IOException, InterruptedException {

        int count = k;
        for (Long key : words.keySet()) {
            if (count > 0) {
                LongWritable frequency = new LongWritable(key);
                context.write(frequency, words.get(key));
                count -= key;
            } else {
                break;
            }
        }
    }

    /*@Override
    protected void cleanup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {

        for(Entry<Long, Text> entry: words.entrySet()) {
            outValue.set(entry.getKey());
            Text word = entry.getValue();
            context.write(word, outValue);
        }

    }*/
}
