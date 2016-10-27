import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Class to try the Java API for Hadoop
 */
public class HadoopAPI extends Configured implements Tool {
    private static final String DIR_PATH = "/kschool/";

    public void createDir(FileSystem fs) throws IOException{
        fs.mkdirs(new Path(DIR_PATH));
    }

    public int run(String[] args) throws Exception {
        // Configuration processed by ToolRunner
        Configuration conf = getConf();

        FileSystem fs = FileSystem.get(conf);

        createDir(fs);

        return 0;
    }

    public static void main(String[] args) throws Exception{
        int res = ToolRunner.run(new HadoopAPI(), args);

        System.exit(res);
    }
}
