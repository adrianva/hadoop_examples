package hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;

import java.io.PrintWriter;
import java.io.IOException;

/**
 * Class to try the Java API for Hadoop
 */
public class HadoopFileSystemAPI extends Configured implements Tool {
    private static final String DIR_PATH = "/kschool/";
    private static final String FILE_PATH = "/kschool/test.txt";
    private static final String LOCAL_PATH = "/Users/adrian/Documents/localfile.txt";

    private void createDir(FileSystem fs) throws IOException {
        fs.mkdirs(new Path(DIR_PATH));
    }

    private void writeFile(FileSystem fs) throws IOException {
        FSDataOutputStream stream = fs.create(new Path(FILE_PATH), true);
        PrintWriter pw = new PrintWriter(stream);
        pw.println("Trying to write in HDFS from Java");
        pw.close();
    }

    private void deleteFile(FileSystem fs) throws IOException {
        fs.delete(new Path(FILE_PATH), false);
    }

    private void get(FileSystem fs) throws IOException {
        fs.copyToLocalFile(new Path(FILE_PATH), new Path(LOCAL_PATH));
    }

    private void put(FileSystem fs) throws IOException {
        fs.copyFromLocalFile(new Path(LOCAL_PATH), new Path(FILE_PATH));
    }
    
    public int run(String[] args) throws Exception {
        // Configuration processed by ToolRunner
        Configuration conf = getConf();

        FileSystem fs = FileSystem.get(conf);

        this.createDir(fs);
        this.writeFile(fs);

        return 0;
    }

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new HadoopFileSystemAPI(), args);

        System.exit(res);
    }
}
