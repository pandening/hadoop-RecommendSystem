package net.hujian.hadoop.recommend;

import net.hujian.hadoop.hdfs.utils.HadoopHdfsOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by hujian on 16-8-28.
 * this mapper/reducer will get the integration needed information like this=>
 * user    itemID,CooNum*Score......
 * user    itemID,CoNum*Score.......
 * so,we just need another map/reducer to integration the result
 */
public class GetIntegrationInfoListMapperReducer{
    /**
     * this is the mapper.just write the info to reducer like this=>
     * if this is a data from cooccurrence file:
     * key:item1 id value:item2,cooNum
     * if this is a data from score list file:
     * key:tem  value:userID,score
     */
    public static class GetIntegrationInfoListMapper extends Mapper<LongWritable,Text,Text,Text>{
        /**
         * i need to know the input file name to decide the type of data
         */
        private FileSplit split;

        /**
         * just emit the data to reducer
         * @param key the file position
         * @param value  the data
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key,Text value,Context context)
            throws IOException,InterruptedException{
            /**
             * get the file name
             */
            split=(FileSplit)context.getInputSplit();
            String filename=split.getPath().getParent().getName().toString();
            /**
             * this is the data array after spilt
             */
            String[] kvs=value.toString().split("\t");
            /**
             * cooccurrence file
             */
            if(filename.equals("out2")){
                String item1=kvs[0].split(":")[0];
                String item2=kvs[0].split(":")[1];
                String cooNum=kvs[1];
                /**
                 * create the key/value
                 * add 'A' to means coo
                 */
                Text k=new Text(item1);
                Text v=new Text("A:"+item2+","+cooNum);
                /**
                 * write to reducer
                 */
                context.write(k,v);
            }
            /**
             * user score list file
             */
            else if(filename.equals("out3")){
                String item=kvs[0];
                String user=kvs[1].split(":")[0];
                String score=kvs[1].split(":")[1];
                /**
                 * create the key/value
                 * add 'B' to means the user score list
                 */
                Text k=new Text(item);
                Text v=new Text("B:"+user+","+score);
                /**
                 * write to reducer
                 */
                context.write(k,v);
            }
            /**
             * error
             */
            else{
                System.out.println("Error File on GetIntegrationInfoListMapperReducer.java");
                System.out.println("|-the file name is:" + filename);
                System.exit(1);
            }
        }
    }

    /**
     * this is the reducer,i will get the data like this
     * user    itemID,CooNum*Score......
     * user    itemID,CoNum*Score.......
     */
    public static class GetIntegrationInfoListReducer extends Reducer<Text,Text,Text,Text>{
        /**
         * get the data from mapper,then calc the recommend result.
         * @param key the item id
         * @param values the A/B data list
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key,Iterable<Text> values,Context context)
            throws IOException,InterruptedException{
            //we need 2 map to store the need data
            //if this data type is 'A' store like this=>
            //map<item,cooNum>
            //if this data type is 'B',store like this=>
            //map<item,score>
            Map<String,String> mapIC=new HashMap<String, String>();
            Map<String,String> mapUS=new HashMap<String, String>();

            //so,let's start to fill the maps
            for(Text vs:values) {
                String val = vs.toString();
                //coo
                if (val.startsWith("A")) {
                    mapIC.put(val.substring(2).split(",")[0],
                            val.substring(2).split(",")[1]);
                }
                //score list
                else {
                    mapUS.put(val.substring(2).split(",")[0],
                            val.substring(2).split(",")[1]);
                }
            }
            /**
             * not,generate the data like this->
             * user item,score*coo...
             */
            double score_cooNum=0;
            Iterator<String> itrIC=mapIC.keySet().iterator();
            while(itrIC.hasNext()){
                //get the item
                String itemIC=itrIC.next();
                //get the cooNum
                int cooNum=Integer.parseInt(mapIC.get(itemIC));
                Iterator<String> itrIS=mapUS.keySet().iterator();
                while(itrIS.hasNext()){
                    //get the user
                    String user=itrIS.next();
                    //get the score
                    double score=Double.parseDouble(mapUS.get(user));
                    //get the mul result
                    score_cooNum=cooNum*score;
                    /**
                     * ok,write this to file
                     */
                    Text k=new Text(user);
                    Text v=new Text(itemIC+","+score_cooNum);
                    context.write(k,v);
                }
            }
        }
    }

    /**
     * ok,let's run it to get the ready data for integration
     * @param paths the in/out paths
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void run(Map<String,String> paths)
    throws  IOException,InterruptedException,ClassNotFoundException{
        /**
         * get the config
         */
        Configuration config=MainRunner.config();
        /**
         * get a hdfs handler
         */
        HadoopHdfsOperations hdfs=new HadoopHdfsOperations(config);
        /**
         * get the in/out path
         */
        String in_A=paths.get("in_A");
        String in_B=paths.get("in_B");

        while(!hdfs.isExist(in_A)||!hdfs.isExist(in_B)){
            System.out.println("[GetIntegrationInfoListMapperReducer]do not has the input file..");
            Thread.sleep(1000);
        }

        //System.out.println("in_A:"+in_A+"\nin_B:"+in_B);
        /**
         * i need to ensure the file name
         */
        //hdfs.rename(in_A,"cooccurrenceOut");
        //hdfs.rename(in_B,"userScore");

        String out=paths.get("preDataOut");
        /**
         * just delete the output file
         */
        hdfs.rm(out);
        //get the job
        Job job=Job.getInstance(config,"Integration_pre");

        //set the map/combine/reduce,but this map/reduce just has the mapper
        job.setMapperClass(GetIntegrationInfoListMapper.class);
        job.setReducerClass(GetIntegrationInfoListReducer.class);

        //set the output type of mapper/reducer
        //default->map/reduce output type is same.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //set the input/output file dir
        FileInputFormat.setInputPaths(job,new Path(in_A),new Path(in_B));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.waitForCompletion(true);
    }
}
