package demo.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CountMapReduceTest {
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        Mapper mapper = new CountMapper();
        Reducer reducer = new CountReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text("hello hello ok ok"));
        List<Pair<Text, IntWritable>> outputs = new ArrayList<Pair<Text, IntWritable>>();
        outputs.add(new Pair<Text, IntWritable>(new Text("hello"), new IntWritable(2)));
        outputs.add(new Pair<Text, IntWritable>(new Text("ok"), new IntWritable(2)));
        mapReduceDriver.withAllOutput(outputs);
        mapReduceDriver.runTest();
    }
}
