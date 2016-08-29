package net.hujian.hadoop.recommend;

import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hujian on 16-8-29.
 * this class is a 'sum' class,just start total map/reduce job
 * set the path is the main work for this class.
 */
public class MainRunner {
    /**
     * run
     * @param args you just need to offer the source data
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void main(String[] args)
    throws IOException,InterruptedException,ClassNotFoundException{
        if(args.length!=1){
            System.out.println("Usage:jar-name source-data-file-name");
            System.exit(1);
        }
        Map<String,String> paths=new HashMap<String, String>();
        //1.set the source data file/and out file
        paths.put("sourceInput",args[0]);
        paths.put("sourceOut","out1");
        UserMarkListMapperReducer.run(paths);

        //2.set the cooccurrence map/reducer's in/out
        paths.put("CooIn","out1");
        paths.put("CooOut","out2");
        CreateCooccurrenceMatrixMapperReducer.run(paths);

        //3.set mix matrix in/out
        paths.put("UtoIIN","out1");
        paths.put("UtoIOut","out3");
        MixMatrixMapperReducer.runUISTOIUS(paths);

        //4.set the pre-integration path
        paths.put("in_A","out2");
        paths.put("in_B","out3");
        paths.put("preDataOut","out4");
        GetIntegrationInfoListMapperReducer.run(paths);

        //the integration map/reduce path
        paths.put("R_IN","out4");
        paths.put("R_OUT","out5");
        IntegrationMapperReducer.run(paths);

        System.out.println("End of Running.");
    }

    /**
     * just get a config
     * @return
     */
    public static JobConf config(){
        JobConf conf=new JobConf(MainRunner.class);
        conf.setJobName("Hadoop-Recommend");
        return conf;
    }
}
