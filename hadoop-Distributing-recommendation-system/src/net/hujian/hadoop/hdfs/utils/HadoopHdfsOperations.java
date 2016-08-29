package net.hujian.hadoop.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created by hujian on 16-8-28.
 * the hadoop hdfs operations handler
 */
public class HadoopHdfsOperations {
    /**
     * this is the my hdfs address,you should offer your hadoop address
     */
    private static final String HDFS="hdfs://localhost:9000";
    /**
     * configure
     */
    private Configuration config;
    /**
     * hadoop server
     */
    private String hdfsServer;

    /**
     * get the config,and this is the default hdfs server
     * @param conf
     */
    public HadoopHdfsOperations(Configuration conf){
        /**
         * call more constructor
         */
        this(HDFS,conf);
    }

    /**
     * you should use this constructor
     * @param hdfs
     * @param conf
     */
    public HadoopHdfsOperations(String hdfs,Configuration conf){
        this.config=conf;
        this.hdfsServer=hdfs;
    }

    /**
     * make a director in my hdfs
     * @param folder director name
     * @throws IOException
     */
    public void mkdirs(String folder)throws IOException{
        Path path=new Path(folder);
        FileSystem fs=FileSystem.get(java.net.URI.create(this.hdfsServer),this.config);
        /**
         * check the dir
         */
        if(!fs.exists(path)){
            fs.mkdirs(path);
        }
        /**
         * close the fs
         */
        fs.close();
    }

    /**
     * rm -r
     * @param folder you want to delete the dir
     * @throws IOException
     */
    public void rm(String folder) throws IOException{
        Path path=new Path(folder);
        FileSystem fs=FileSystem.get(java.net.URI.create(this.hdfsServer),this.config);
        fs.deleteOnExit(path);
        /**
         * close this fs
         */
        fs.close();
    }

    /**
     * re name a file
     * @param oldN old name
     * @param newN new name
     * @throws IOException
     */
    public void rename(String oldN,String newN) throws IOException{
        Path oldP=new Path(oldN);
        Path newP=new Path(newN);
        /**
         * get the file system
         */
        FileSystem fs=FileSystem.get(java.net.URI.create(this.hdfsServer),this.config);
        /**
         * rename old to new
         */
        fs.rename(oldP,newP);
        /**
         * close the fs
         */
        fs.close();
    }

    /**
     * just like ls in unix
     * @param folder you want to show this dir
     * @throws IOException
     */
    public void ls(String folder) throws  IOException{
        Path path=new Path(folder);
        FileSystem fs=FileSystem.get(java.net.URI.create(this.hdfsServer),this.config);
        /**
         * show the dir
         */
        for(FileStatus file:fs.listStatus(path)){
            System.out.println(file.getPath()+" *.*\t"+file.getLen());
        }
        /**
         * close this fs
         */
        fs.close();
    }

    /**
     * create a file,then write to it the {content}
     * @param file  the file name
     * @param content the content
     * @throws IOException
     */
    public void createFile(String file,String content) throws IOException{
        /**
         * get the filesystem
         */
        FileSystem fs=FileSystem.get(java.net.URI.create(this.hdfsServer),this.config);
        /**
         * the bytes you want to write to file.
         */
        byte[] buffer=content.getBytes();
        /**
         * output handler
         */
        FSDataOutputStream os = null;
        try {
            os = fs.create(new Path(file));
            os.write(buffer, 0, buffer.length);
        }finally {
            if(os!=null){
                os.close();
            }
        }
        fs.close();
    }

    /**
     * copy file from from to to
     * @param from the from file name
     * @param to the to file name
     * @throws IOException
     */
    public void copyFile(String from,String to) throws IOException{
        FileSystem fs=FileSystem.get(java.net.URI.create(this.hdfsServer),this.config);
        fs.copyFromLocalFile(new Path(from),new Path(to));
        fs.close();
    }

    /**
     * you want to get a file from remote to local
     * @param remoteSrc remote file
     * @param localTo dest file
     * @throws IOException
     */
    public void download(String remoteSrc,String localTo)throws IOException{
        FileSystem fs=FileSystem.get(java.net.URI.create(this.hdfsServer),this.config);
        fs.copyToLocalFile(new Path(remoteSrc),new Path(localTo));
        fs.close();
    }

    /**
     * cat show
     * @param file the file
     * @return
     * @throws IOException
     */
    public String cat(String file)throws IOException{
        Path path=new Path(file);
        FileSystem fs=FileSystem.get(java.net.URI.create(this.hdfsServer),this.config);
        FSDataInputStream is=null;
        OutputStream os=new ByteArrayOutputStream();
        try{
            is=fs.open(path);
            IOUtils.copyBytes(is,os,4096,false);
            return os.toString();
        }finally {
            IOUtils.closeStream(is);
            fs.close();
        }
    }

    /**
     * judge if exist
     * @param file the file name
     * @return
     */
    public boolean isExist(String file) throws IOException{
        Path path=new Path(file);
        FileSystem fs=FileSystem.get(URI.create(this.hdfsServer),this.config);
        return fs.exists(path);
    }
}
