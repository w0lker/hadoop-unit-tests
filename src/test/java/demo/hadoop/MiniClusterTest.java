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

import static org.junit.Assert.assertTrue;

public class MiniClusterTest {
    private Configuration configuration;
    private MiniDFSCluster dfsCluster;
    private MiniYARNCluster yarnCluster;
    private FileSystem fs;

    @Before
    public void setUp() throws IOException {
        configuration = new Configuration();
        startHDFS();
        startYARN();
        fs = FileSystem.get(configuration);
    }

    @After
    public void tearDown() {
        if (dfsCluster != null) {
            dfsCluster.shutdown();
        }
    }

    @Test
    public void testStartJob() throws InterruptedException, IOException, ClassNotFoundException {
        Path hdfsIn = new Path("/hello");
        Path hdfsOut = new Path("/out");
        copyFileToHDFS(new Path(new File("data/wordcount").getAbsolutePath()), hdfsIn);

        Job job = Job.getInstance(configuration);
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, hdfsIn);
        FileOutputFormat.setOutputPath(job, hdfsOut);
        job.waitForCompletion(true);
        assertTrue(job.isSuccessful());

        printOutputResult(hdfsOut);
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

    private void copyFileToHDFS(Path localFile, Path hdfsIn) throws IOException {
        fs.copyFromLocalFile(false, true, localFile, hdfsIn);
    }

    private void printOutputResult(Path hdfsOut) throws IOException {
        FSDataInputStream inputStream = fs.open(new Path(hdfsOut, "part-r-00000"));
        System.out.println("Output:");
        System.out.println(IOUtils.toString(inputStream));
    }

}
