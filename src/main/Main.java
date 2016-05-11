package main;

import org.apache.hadoop.mapred.lib.ChainReducer;
import testing.TestMapper;
import testing.TestReducer;
import training.TrainingMapper;
import training.TrainingReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import java.io.File;


public class Main extends Configured implements Tool{
    public int run(String[] args) throws Exception
    {
        JobConf jobTrain = createTrainingJob(args);
        JobConf jobTest = createTestJob(args);


        JobClient.runJob(jobTrain);
        JobClient.runJob(jobTest);
        return 0;
    }

    private JobConf createTrainingJob(String[] args) {
        //creating a JobConf object and assigning a job name for identification purposes
        JobConf jobTrain = new JobConf(getConf(), Main.class);
        jobTrain.setJobName("main.Main");

        //Setting configuration object with the Data Type of output Key and Value
        jobTrain.setOutputKeyClass(Text.class);
        jobTrain.setOutputValueClass(Text.class);

        //Providing the mapper and reducer class names
        jobTrain.setMapperClass(TrainingMapper.class);
        jobTrain.setReducerClass(TrainingReducer.class);

        jobTrain.setStrings("temporaryOutput", args[2]);
        int numberOfVar = Integer.parseInt(args[3]);
        int numberOfRecords = Integer.parseInt(args[4]);

        String delimiter =args[5];

        jobTrain.setInt("numberOfVariables", numberOfVar);
        jobTrain.setInt("numberOfRecords", numberOfRecords);
        jobTrain.setStrings("delimiter", delimiter);

        String [] attr = new String[numberOfVar];
        for(int i =0; i< numberOfVar; i++){
            attr[i] =  args[7 + i];
        }

        jobTrain.setStrings("variables", attr);

        //We wil give 2 arguments at the run time, one in input path and other is output path
        Path inp = new Path(args[0]);
        Path out = new Path(args[1]);

        //the hdfs input and output directory to be fetched from the command line
        FileInputFormat.addInputPath(jobTrain, inp);
        FileOutputFormat.setOutputPath(jobTrain, out);
        return jobTrain;
    }


    private JobConf createTestJob(String[] args) {
        //creating a JobConf object and assigning a job name for identification purposes
        JobConf jobTest = new JobConf(getConf(), Main.class);
        jobTest.setJobName("Test");

        //Setting configuration object with the Data Type of output Key and Value
        jobTest.setOutputKeyClass(Text.class);
        jobTest.setOutputValueClass(Text.class);

        //Providing the mapper and reducer class names
        jobTest.setMapperClass(TestMapper.class);
        jobTest.setReducerClass(TestReducer.class);



        //We wil give 2 arguments at the run time, one in input path and other is output path
        Path inp = new Path(args[1]);
        Path out = new Path(args[6]);

        //the hdfs input and output directory to be fetched from the command line
        FileInputFormat.addInputPath(jobTest, inp);
        FileOutputFormat.setOutputPath(jobTest, out);
        return jobTest;
    }


    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }



    public static void main(String[] args) throws Exception
    {

        //delete file outt (Hadoop error-  file already exists!!!)
        File file = new File(args[1]);
        deleteDir(file);
        // this main function will call run method defined above.
        int res = ToolRunner.run(new Configuration(), new Main(),args);
        System.exit(res);
    }
}