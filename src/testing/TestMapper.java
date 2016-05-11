package testing;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * Created by hadoop on 5/9/16.
 */
public class TestMapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<Text, Text, Text, DoubleWritable> {


    @Override
    public void map(Text text, Text text2, OutputCollector<Text, DoubleWritable> textDoubleWritableOutputCollector, Reporter reporter) throws IOException {

        System.out.println("Hello from the second Test Mapper! ");
    }
}
