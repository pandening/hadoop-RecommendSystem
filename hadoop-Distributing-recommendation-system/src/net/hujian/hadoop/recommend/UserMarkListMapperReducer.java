package net.hujian.hadoop.recommend;

import net.hujian.hadoop.hdfs.utils.HadoopHdfsOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * Created by hujian on 16-8-28.
 * this map/reduce will get the user-score list from input file
 * and,the input file need like this format=>
 * userID,ItemID,Score
 * example=>
 * 1,10001,8.3
 * 1,10002,3.2
 * 2,10001,2.3
 * .......
 */
public class UserMarkListMapperReducer {
    /**
     * this is the mapper,just emit the data.
     */
    public static class UserMarkListMapper extends Mapper<Object,Text,Text,Text>{
        /**
         * the output key and value
         */
        private Text keyOut=new Text();
        private Text valueOut=new Text();
        /**
         * the map function,i will split the input,then get the userID,and the itemID,Score
         * then write to local by this format:userID ItemID:score
         * so,after the hadoop shuffle process,the reducer will get the format:
         * userID itemID:score....
         * @param keyIn  the input file's position
         * @param valueIn the real care data
         * @param context output collector
         * @throws IOException if something wrong while I/O
         * @throws InterruptedException interrupted by something(operation person .etc)
         */
        public void map(Object keyIn,Text valueIn,Context context)
                throws IOException,InterruptedException{
            String[] dataItems=valueIn.toString().split(",");
            /**
             * ok,get the key out and value out
             */
            keyOut.set(dataItems[0]);
            valueOut.set(dataItems[1]+":"+dataItems[2]);
            /**
             * write to local
             */
            context.write(keyOut,valueOut);
        }
    }
    /**
     * the reducer
     */
    public static class UserMarkListReducer extends Reducer<Text,Text,Text,Text>{
        /**
         * the out value
         */
        private Text valueOut=new Text();
        /**
         * the reducer function,just get the list we need
         * @param keyIn the user id
         * @param valueIn the list of item and score
         * @param context output collector
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text keyIn,Iterable<Text>valueIn,Context context)
                throws IOException,InterruptedException {
            StringBuilder sb=new StringBuilder();
            for(Text line:valueIn){
                sb.append(line.toString()+",");
            }
            /**
             * remove the last ','
             */
            sb=new StringBuilder(sb.toString().substring(0,sb.toString().length()-1));
            valueOut.set(sb.toString());
            /**
             * output to hdfs
             */
            context.write(keyIn,valueOut);
        }
    }

    /**
     * run this hadoop map/reduce job
     * @param paths the in/out path
     * @throws IOException
     */
    public static void run(Map<String,String> paths)
            throws IOException,InterruptedException,ClassNotFoundException{
        /**
         * get the config
         */
        Configuration config=MainRunner.config();
        /**
         * get the in/out path
         */
        String in=paths.get("sourceInput");
        String out=paths.get("sourceOut");
        /**
         * get a hdfs handler
         */
        HadoopHdfsOperations hdfs=new HadoopHdfsOperations(config);
        while(!hdfs.isExist(in)){
            System.out.println("[UserMarkListMapperReducer]do not has the input file..");
            Thread.sleep(1000);
        }
        /**
         * just delete the output file
         */
        hdfs.rm(out);
        //get the job
        Job job=Job.getInstance(config,"MarkList");

        //set the map/combine/reduce
        job.setMapperClass(UserMarkListMapper.class);
        job.setReducerClass(UserMarkListReducer.class);

        //set the output type of mapper/reducer
        //default->map/reduce output type is same.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //set the input/output file dir
        FileInputFormat.addInputPath(job,new Path(in));
        FileOutputFormat.setOutputPath(job,new Path(out));

        job.waitForCompletion(true);
    }
}
