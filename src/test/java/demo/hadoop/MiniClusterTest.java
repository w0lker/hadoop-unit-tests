package demo.hadoop;

import com.google.common.io.Files;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class MiniClusterTest {
    private Configuration configuration = new Configuration();
    private MiniDFSCluster dfsCluster;
    private MiniYARNCluster yarnCluster;

    @Before
    public void setUp() throws IOException {
        configuration.addResource(new Path(new File("conf.xml").getAbsolutePath().toString()));
    }

    @After
    public void tearDown() {
        if (dfsCluster != null) {
            dfsCluster.shutdown();
        }
    }

    private void startHDFS() throws IOException {
        File tempDir = Files.createTempDir();
        configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.getAbsolutePath());
        dfsCluster = new MiniDFSCluster.Builder(configuration).numDataNodes(3).nameNodePort(9000).build();
    }

    private void startYARN() throws IOException {
        yarnCluster = new MiniYARNCluster("test", 1, 1, 1);
        yarnCluster.init(configuration);
        yarnCluster.start();
    }

    @Test
    public void testStartHDFS() throws InterruptedException, IOException {
        startHDFS();
        while (true) {
            Thread.sleep(1000);
        }
    }

    @Test
    public void testStartJob() throws InterruptedException, IOException, ClassNotFoundException {
        Path in = new Path("/hello");
        Path out = new Path("/out");
        copyFileToHDFS(in);

        Job job = Job.getInstance(configuration);
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        job.waitForCompletion(true);

        printOutput(out);
    }

    private void copyFileToHDFS(Path in) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        fs.copyFromLocalFile(
                false,
                true,
                new Path(new File("data/wordcount").getAbsolutePath()),
                in
        );
    }

    private void printOutput(Path out) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        FSDataInputStream inputStream = fs.open(new Path(out, "part-r-00000"));
        System.out.println("Output:");
        System.out.println(IOUtils.toString(inputStream));
    }

}
