package net.hujian.hadoop.recommend;

import net.hujian.hadoop.hdfs.utils.HadoopHdfsOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * Created by hujian on 16-8-28.
 * just mix the matrix(cooccurrence matrix and user-score list.)
 */
public class MixMatrixMapperReducer {
    /**
     * the mapper1,just translate the data format userID  item:score,item:score...
     * to item   user:score
     */
    public static class MixMatrixUISToIUSMapper extends Mapper<LongWritable,Text,Text,Text>{
        /**
         * this is the mapper to translate the data format
         * @param key the file position
         * @param value the content
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(LongWritable key,Text value,Context context)
            throws IOException,InterruptedException{
            /**
             * index 0 means the user id
             * index 1 means the item:score list
             */
            String[] itemScoreList=value.toString().split("\t");
            /**
             * i just need the score list
             */
            String[] scoreList=itemScoreList[1].split(",");
            for(int i=0;i<scoreList.length;i++){
                /**
                 * get the item id and the score,the key is the item id
                 * the value is the user:score
                 */
                context.write(new Text(scoreList[i].split(":")[0]),
                        new Text(itemScoreList[0]+":"+scoreList[i].split(":")[1]));
            }
        }
    }

    /**
     * run the mapper up,just get the new format file
     * @param paths the in.out file path
     * @throws IOException
     * @throws InterruptedException
     */
    public static void runUISTOIUS(Map<String,String> paths)
        throws IOException,InterruptedException,ClassNotFoundException{
        /**
         * get the config
         */
        Configuration config=MainRunner.config();
        /**
         * get the in/out path
         */
        String in=paths.get("UtoIIN");
        String out=paths.get("UtoIOut");
        /**
         * get a hdfs handler
         */
        HadoopHdfsOperations hdfs=new HadoopHdfsOperations(config);
        while(!hdfs.isExist(in)){
            System.out.println("[MixMatrixMapperReducer]do not has the input file..");
            Thread.sleep(1000);
        }
        /**
         * just delete the output file
         */
        hdfs.rm(out);
        //get the job
        Job job=Job.getInstance(config,"MatrixUtoI");

        //set the map/combine/reduce,but this map/reduce just has the mapper
        job.setMapperClass(MixMatrixUISToIUSMapper.class);

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
