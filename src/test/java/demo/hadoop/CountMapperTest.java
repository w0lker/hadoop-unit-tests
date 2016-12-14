package demo.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class CountMapperTest {
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    @Before
    public void setUp() {
        Mapper mapper = new CountMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("hello world good hello"));
        mapDriver.withOutput(new Text("hello"), new IntWritable(1));
        mapDriver.withOutput(new Text("world"), new IntWritable(1));
        mapDriver.withOutput(new Text("good"), new IntWritable(1));
        mapDriver.withOutput(new Text("hello"), new IntWritable(1));
        mapDriver.runTest();
    }
}
