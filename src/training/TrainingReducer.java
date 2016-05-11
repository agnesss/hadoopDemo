package training;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.*;
import java.util.*;
import utils.*;
import utils.Utils;

/**
 * Created by hadoop on 5/7/16.
 */
public class TrainingReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text> {


    private static String PATH_FILE;
    private File tempFile ;
    private BufferedWriter bf;
    Map<String, Counter> smallMap;
    private static int NUMBER_OF_RECORDS;
    public static String DELIMITER;


    @Override
    public void configure(JobConf job) {
        super.configure(job);


        PATH_FILE = job.getStrings("temporaryOutput", "undefined")[0];
        NUMBER_OF_RECORDS =  job.getInt("numberOfRecords", 0);
        DELIMITER =  job.getStrings("delimiter", ".")[0];
        tempFile = new File(PATH_FILE);
        try {
            bf = new BufferedWriter(new FileWriter(new File(PATH_FILE)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            //System.out.println(" ----------------- !!!!!!!!!!! ----------------");
        }
    }

    @Override
    public void reduce(Text text, Iterator<Text> textIterator, OutputCollector<Text, Text> textTextOutputCollector, Reporter reporter) throws IOException {


        //we create the set in order to establish what are the options corresponding to each variable(attribute) considered
        //for classification. Ex: for variable Outlook, we have the options: sunny, rain, mild.


        StringBuffer buf = new StringBuffer();
        int size = 0;

       while(textIterator.hasNext()){
           buf.append((textIterator.next()+" "));
           //size++;
       }

        //textTextOutputCollector.collect(text, new Text(buf.toString()));

        Utils.processDataFlow(text.toString(),buf.toString());
        //clean the buffer
        //buf.delete(0, buf.capacity());
        StringBuffer newBuf = new StringBuffer();
        newBuf.append(text.toString()+":");
        Map<String, Counter> optMap = Utils.CLASSES.get(text.toString());
        Set<String> attributeOptions = optMap.keySet();
        for(String option :  attributeOptions){
            Counter counter = optMap.get(option);
            newBuf.append(option+" "+counter.countAttribute+" "+counter.countYes+" "+counter.countNo+"|");
        }


        textTextOutputCollector.collect(text, new Text(newBuf.toString()));




       /* if(size == numberOfRecords){  //it means we are processing an attribute, not an attribute option

            Utils.processAttribute(text.toString(), buf.toString());
        }
        else {
            //Utils.countNumberOfOptions(text.toString(), buf.toString());

        }*/
/*

        while(textIterator.hasNext()){

            attributeOptionsSet.add(textIterator.next().toString());
        }

        bf.write(text+" "+pretty(attributeOptionsSet)+"\n");
        bf.flush();

        smallMap = new HashMap<String, Counter>();

        for(String option : attributeOptionsSet){
            smallMap.put(option, null);
        }

        Utils utils = new Utils();
        Utils.CLASSES.put(text.toString(), smallMap);*/
        //textTextOutputCollector.collect(text, new Text(pretty(attributeOptionsSet)));


    }

    /*private String pretty(Set<String> samples){
        StringBuffer buf = new StringBuffer();

        for(String str : samples){
            buf.append(str+" ");
        }
        return buf.toString();
    }*/


}
