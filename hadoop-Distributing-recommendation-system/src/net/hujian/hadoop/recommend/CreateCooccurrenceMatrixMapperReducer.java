package net.hujian.hadoop.recommend;

import net.hujian.hadoop.hdfs.utils.HadoopHdfsOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * Created by hujian on 16-8-28.
 * this mapper/reducer will create the cooccurrence matrix
 */
public class CreateCooccurrenceMatrixMapperReducer {
    /**
     * the mapper
     */
    public static class CreateCooccurrenceMatrixMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        /**
         * the out key/value
         */
        private Text outKey=new Text();
        private IntWritable outValue=new IntWritable(1);
        /**
         * the mapper,i just care the item,user just ignore
         * @param key file position
         * @param values the format just like this=>userID itemID:score,itemID:score
         * @param context
         */
        public void map(LongWritable key,Text values,Context context)
        throws IOException,InterruptedException{
            /**
             * get the item score list
             */
            String[] itemInfoList=values.toString().split("\t")[1].split(",");
            for(int i=0;i<itemInfoList.length;i++){
                /**
                 * index 0 means the item id,the index 1 means the score
                 */
                String item1=itemInfoList[i].split(":")[0];
                for(int j=0;j<itemInfoList.length;j++){
                    String item2=itemInfoList[j].split(":")[0];
                    outKey.set(item1+":"+item2);
                    context.write(outKey,outValue);
                }
            }
        }
    }
    /**
     * the reducer
     */
    public static class CreateCooccurrenceMatrixReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        /**
         * the cooccurrence result
         */
        private IntWritable outValue=new IntWritable();

        /**
         * the reducer,just get the cooccurrence matrix
         * @param key the itemID:itemID
         * @param values int list
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key,Iterable<IntWritable>values,Context context)
            throws IOException,InterruptedException{
            int cooccurrenceSum=0;
            /**
             * get the cooccurrence result
             */
            for(IntWritable coo:values){
                cooccurrenceSum+=coo.get();
            }
            /**
             * set the value and output
             */
            outValue.set(cooccurrenceSum);
            context.write(key,outValue);
        }
    }

    /**
     * run this map/reduce job
     * @param paths get the input/output path
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void run(Map<String,String> paths)
            throws IOException,InterruptedException,ClassNotFoundException{
        /**
         * get the config
         */
        JobConf config=MainRunner.config();
        /**
         * get the in/out path
         */
        String in=paths.get("CooIn");
        String out=paths.get("CooOut");
        /**
         * get a hdfs handler
         */
        HadoopHdfsOperations hdfs=new HadoopHdfsOperations(config);
        //wait to get the input file
        while(!hdfs.isExist(in)){
            System.out.println("[CreateCooccurrenceMatrixMapperReducer]do not has the input file..");
            Thread.sleep(1000);
        }
        /**
         * just delete the output file
         */
        hdfs.rm(out);
        //get the job
        Job job=Job.getInstance(config,"CooccurrenceMatrix");

        //set the map/combine/reduce
        job.setMapperClass(CreateCooccurrenceMatrixMapper.class);
        job.setReducerClass(CreateCooccurrenceMatrixReducer.class);

        //set the output type of mapper/reducer
        //default->map/reduce output type is same.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //set the input/output file dir
        FileInputFormat.addInputPath(job,new Path(in));
        FileOutputFormat.setOutputPath(job,new Path(out));
        //run this job
        job.waitForCompletion(true);
    }
}
