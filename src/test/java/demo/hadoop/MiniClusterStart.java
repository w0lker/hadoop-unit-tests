package demo.hadoop;

import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.File;
import java.io.IOException;

public class MiniClusterStart {

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);

        File tempDir = Files.createTempDir();
        configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.getAbsolutePath());
        new MiniDFSCluster.Builder(configuration).numDataNodes(1).nameNodePort(9000).nameNodeHttpPort(50070).build();

        //MiniYARNCluster yarnCluster = new MiniYARNCluster("test", 1, 1, 1);
        //yarnCluster.init(configuration);
        //yarnCluster.start();
    }

}
