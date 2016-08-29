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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by hujian on 16-8-29.
 * this is the last mapper/reducer of this project.
 * get the data item from pre-integration reducer like this=>
 * user   item:cooNum*score list
 * i just reduce the same user.
 */
public class IntegrationMapperReducer {
    /**
     * this is the mapper part,just emit data to reducer
     */
    public static class IntegrationMapper extends Mapper<LongWritable,Text,Text,Text>{
        /**
         * map
         * @param key file position
         * @param value real data
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key,Text value,Context context)
            throws IOException,InterruptedException{
            //k,v->write
            context.write(new Text(value.toString().split("\t")[0]),
                    new Text(value.toString().split("\t")[1]));
            //System.out.println("get data->" + value.toString());
        }
    }
    /**
     * this is the map,just like wordcount
     */
    public static class IntegrationReducer extends Reducer<Text,Text,Text,Text>{
        /**
         * reducer.integration the recommend result
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key,Iterable<Text> values,Context context)
            throws IOException,InterruptedException{
            Map<String,Double> result=new HashMap<String, Double>();
            for(Text vls:values){
                //get the item id
                String item=vls.toString().split(",")[0];
                //get the score
                Double score=Double.parseDouble(vls.toString().split(",")[1]);
                //System.out.println("item=" + item + " score=" + score);
                if(result.containsKey(item)){
                    result.put(item,result.get(item)+score);
                }else{
                    result.put(item,score);
                }
            }
            /**
             * generator..
             */
            Iterator<String> itr=result.keySet().iterator();
            while(itr.hasNext()){
                /**
                 * get the result,then write to hdfs
                 * key is the user
                 * v is=>item:recommend score
                 */
                String item=itr.next();
                context.write(key,new Text(item+":"+result.get(item)));
            }
        }
    }

    /**
     * run.
     * @param paths
     * @throws IOException
     * @throws InterruptedException
     */
    public static void run(Map<String,String> paths)
    throws IOException,InterruptedException,ClassNotFoundException{
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
        String in=paths.get("R_IN");
        String out=paths.get("R_OUT");
        while(!hdfs.isExist(in)){
            System.out.println("[IntegrationMapperReducer]do not has the input file..");
            Thread.sleep(1000);
        }
        /**
         * just delete the output file
         */
        hdfs.rm(out);
        //get the job
        Job job=Job.getInstance(config,"Integration");

        //set the map/combine/reduce,but this map/reduce just has the mapper
        job.setMapperClass(IntegrationMapper.class);
        job.setReducerClass(IntegrationReducer.class);

        //set the output type of mapper/reducer
        //default->map/reduce output type is same.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //set the input/output file dir
        FileInputFormat.setInputPaths(job,new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.waitForCompletion(true);
    }
}
