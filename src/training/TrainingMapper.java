/**
 * Created by hadoop on 5/7/16.
 */
package training;
import java.io.IOException;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapred.*;

public class TrainingMapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, Text>
{
    //define data types used
    private Text attribute = new Text();
    private String  [] variables;
    private Integer size;
    private Text attrValue = new Text();


    @Override
    public void configure(JobConf job) {
        super.configure(job);
        size = job.getInt("numberOfVariables", 0);

        //get comma separated the variables taken into account for classification
        //if no value is provided, then default value will be returned ("undefined" in this case)
        variables = job.getStrings("variables", "undefined");
    }



    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter ) throws IOException
    {

        //processing each line from the file and tokenize it
        String line = value.toString();
        String [] tokens  = line.split(" ");

        //we iterate through the tokens available and create (key, value) pairs
        for(int i=0; i<tokens.length; i++){

            attribute.set(variables[i]);
            //attrValue.set(tokens[i]);
            output.collect(attribute, new Text(tokens[i]+"-"+tokens[tokens.length -1]));

        }


    }
}
