package distributedgrep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
public class DistributedGrepDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        // Configuration processed by ToolRunner
        Configuration conf = getConf();

        final Job job = Job.getInstance(conf);

        // which jar contains the implementation
        job.setJarByClass(DistributedGrepDriver.class);
        // diput ata format
        job.setInputFormatClass(TextInputFormat.class);
        // output data format
        job.setOutputFormatClass(TextOutputFormat.class);
        // Types emitted by map function
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // Where we read the data from
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Where we write the data to
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.getConfiguration().set("grep", args[2]);

        job.setMapperClass(DistributedGrepMapper.class);

        job.setNumReduceTasks(0);

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int res = ToolRunner.run(new DistributedGrepDriver(), args);

        System.exit(res);
    }
}
