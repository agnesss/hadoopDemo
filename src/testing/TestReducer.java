package testing;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by hadoop on 5/9/16.
 */
public class TestReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, DoubleWritable, Text, Text>  {
    @Override
    public void reduce(Text text, Iterator<DoubleWritable> doubleWritableIterator, OutputCollector<Text, Text> textTextOutputCollector, Reporter reporter) throws IOException {

        System.out.println("Hello from second reducer!");
    }
}
