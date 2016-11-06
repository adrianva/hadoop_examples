package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

/**
 * Main class for the wordcount.WordCount in Hadoop
 */
public class TopKWordCountDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        // Configuration processed by ToolRunner
        Configuration conf = getConf();

        Job wordCountJob = this.configureWordCountJob(conf, args[0], args[1]);
        Job topKWordsJob = this.configureTopKJob(conf, args[1], args[2]);

        wordCountJob.waitForCompletion(true);
        wordCountJob.getConfiguration().setInt("top_k", Integer.parseInt(args[3]));

        topKWordsJob.waitForCompletion(true);
        topKWordsJob.getConfiguration().setInt("top_k", Integer.parseInt(args[3]));

        return 0;
    }

    private Job configureWordCountJob(Configuration conf, String inputPath, String outputPath)
            throws Exception {

        final Job job = Job.getInstance(conf);

        // which jar contains the implementation
        job.setJarByClass(WordCountDriver.class);
        // diput ata format
        job.setInputFormatClass(TextInputFormat.class);
        // output data format
        job.setOutputFormatClass(TextOutputFormat.class);
        // Types emitted by map function
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        // Types emitted by reduce funcion
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Where we read the data from
        FileInputFormat.addInputPath(job, new Path(inputPath));

        // Where we write the data to
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        return job;
    }

    private Job configureTopKJob(Configuration conf, String inputPath, String outputPath)
            throws Exception {

        final Job job = Job.getInstance(conf);

        // which jar contains the implementation
        job.setJarByClass(WordCountDriver.class);
        // diput ata format
        job.setInputFormatClass(TextInputFormat.class);
        // output data format
        job.setOutputFormatClass(TextOutputFormat.class);
        // Types emitted by map function
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(TextArrayWritable.class);
        // Types emitted by reduce funcion
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Where we read the data from
        FileInputFormat.addInputPath(job, new Path(inputPath));

        // Where we write the data to
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(TopKWordCountMapper.class);
        job.setReducerClass(TopKWordCountReducer.class);

        return job;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        ToolRunner.run(new TopKWordCountDriver(), args);

        System.exit(0);
    }
}
