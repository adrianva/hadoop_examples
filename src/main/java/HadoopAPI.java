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
public class HadoopAPI extends Configured implements Tool {
    private static final String DIR_PATH = "/kschool/";
    private static final String FILE_PATH = "/kschool/test.txt";

    public void createDir(FileSystem fs) throws IOException {
        fs.mkdirs(new Path(DIR_PATH));
    }

    public void writeFile(FileSystem fs) throws IOException {
        FSDataOutputStream stream = fs.create(new Path(FILE_PATH), true);
        PrintWriter pw = new PrintWriter(stream);
        pw.println("Trying to write in HDFS from Java");
        pw.close();
    }

    public void deleteFile(FileSystem fs) throws IOException {
        fs.delete(new Path(FILE_PATH), false);
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
        int res = ToolRunner.run(new HadoopAPI(), args);

        System.exit(res);
    }
}
