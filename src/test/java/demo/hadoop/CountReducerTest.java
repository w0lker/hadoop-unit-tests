package demo.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CountReducerTest {
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        Reducer reducer = new CountReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        // 假设输入结果为: hello -> [1, 1]
        reduceDriver.withInput(new Text("hello"), values);
        // 如果逻辑正确应该输出: hello -> 2
        reduceDriver.withOutput(new Text("hello"), new IntWritable(2));
        reduceDriver.runTest();
    }
}
