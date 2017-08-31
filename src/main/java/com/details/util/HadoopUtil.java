package com.details.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirValueIterator;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HadoopUtil {

    private static final Logger log = LoggerFactory.getLogger(HadoopUtil.class);

    private HadoopUtil() { }

    public static Job prepareJob(String jobName,
                                 String[] inputPath,
                                 String outputPath,
                                 Class<? extends InputFormat> inputFormat,
                                 Class<? extends Mapper> mapper,
                                 Class<? extends Writable> mapperKey,
                                 Class<? extends Writable> mapperValue,
                                 Class<? extends OutputFormat> outputFormat, Configuration conf) throws IOException {

        Job job = new Job(new Configuration(conf));
        job.setJobName(jobName);
        Configuration jobConf = job.getConfiguration();

        if (mapper.equals(Mapper.class)) {
            throw new IllegalStateException("Can't figure out the user class jar file from mapper/reducer");
        }
        job.setJarByClass(mapper);

        job.setInputFormatClass(inputFormat);
        job.setInputFormatClass(inputFormat);
        StringBuilder inputPathsStringBuilder =new StringBuilder();
        for(String p : inputPath){
            inputPathsStringBuilder.append(",").append(p);
        }
        inputPathsStringBuilder.deleteCharAt(0);
        jobConf.set("mapred.input.dir", inputPathsStringBuilder.toString());

        job.setMapperClass(mapper);
        job.setMapOutputKeyClass(mapperKey);
        job.setMapOutputValueClass(mapperValue);
        job.setOutputKeyClass(mapperKey);
        job.setOutputValueClass(mapperValue);
        jobConf.setBoolean("mapred.compress.map.output", true);
        job.setNumReduceTasks(0);

        job.setOutputFormatClass(outputFormat);
        jobConf.set("mapred.output.dir", outputPath);

        return job;
    }

    public static Job prepareJob(String jobName,
                                 String[] inputPath,
                                 String outputPath,
                                 Class<? extends InputFormat> inputFormat,
                                 Class<? extends Mapper> mapper,
                                 Class<? extends Writable> mapperKey,
                                 Class<? extends Writable> mapperValue,
                                 Class<? extends Reducer> reducer,
                                 Class<? extends Writable> reducerKey,
                                 Class<? extends Writable> reducerValue,
                                 Class<? extends OutputFormat> outputFormat,
                                 Configuration conf) throws IOException {

        Job job = new Job(new Configuration(conf));
        job.setJobName(jobName);
        Configuration jobConf = job.getConfiguration();

        if (reducer.equals(Reducer.class)) {
            if (mapper.equals(Mapper.class)) {
                throw new IllegalStateException("Can't figure out the user class jar file from mapper/reducer");
            }
            job.setJarByClass(mapper);
        } else {
            job.setJarByClass(reducer);
        }

        job.setInputFormatClass(inputFormat);
        StringBuilder inputPathsStringBuilder =new StringBuilder();
        for(String p : inputPath){
            inputPathsStringBuilder.append(",").append(p);
        }
        inputPathsStringBuilder.deleteCharAt(0);
        jobConf.set("mapred.input.dir", inputPathsStringBuilder.toString());

        job.setMapperClass(mapper);
        if (mapperKey != null) {
            job.setMapOutputKeyClass(mapperKey);
        }
        if (mapperValue != null) {
            job.setMapOutputValueClass(mapperValue);
        }

        jobConf.setBoolean("mapred.compress.map.output", true);

        job.setReducerClass(reducer);
        job.setOutputKeyClass(reducerKey);
        job.setOutputValueClass(reducerValue);

        job.setOutputFormatClass(outputFormat);
        jobConf.set("mapred.output.dir", outputPath);

        return job;
    }




    public static Job prepareJob(String jobName, String[] inputPath,
                                 String outputPath, Class<? extends InputFormat> inputFormat,
                                 Class<? extends Mapper> mapper,
                                 Class<? extends Writable> mapperKey,
                                 Class<? extends Writable> mapperValue,
                                 Class<? extends Reducer> combiner,
                                 Class<? extends Reducer> reducer,
                                 Class<? extends Writable> reducerKey,
                                 Class<? extends Writable> reducerValue,
                                 Class<? extends OutputFormat> outputFormat, Configuration conf)
            throws IOException {

        Job job = new Job(new Configuration(conf));
        job.setJobName(jobName);
        Configuration jobConf = job.getConfiguration();

        if (reducer.equals(Reducer.class)) {
            if (mapper.equals(Mapper.class)) {
                throw new IllegalStateException(
                        "Can't figure out the user class jar file from mapper/reducer");
            }
            job.setJarByClass(mapper);
        } else {
            job.setJarByClass(reducer);
        }

        job.setInputFormatClass(inputFormat);
        StringBuilder inputPathsStringBuilder = new StringBuilder();
        for (String p : inputPath) {
            inputPathsStringBuilder.append(",").append(p);
        }
        inputPathsStringBuilder.deleteCharAt(0);
        jobConf.set("mapred.input.dir", inputPathsStringBuilder.toString());

        job.setMapperClass(mapper);
        if (mapperKey != null) {
            job.setMapOutputKeyClass(mapperKey);
        }
        if (mapperValue != null) {
            job.setMapOutputValueClass(mapperValue);
        }

        jobConf.setBoolean("mapred.compress.map.output", true);

        job.setCombinerClass(combiner);

        job.setReducerClass(reducer);
        job.setOutputKeyClass(reducerKey);
        job.setOutputValueClass(reducerValue);

        job.setOutputFormatClass(outputFormat);
        jobConf.set("mapred.output.dir", outputPath);

        return job;
    }

    public static String getCustomJobName(String className, JobContext job,
                                          Class<? extends Mapper> mapper,
                                          Class<? extends Reducer> reducer) {
        StringBuilder name = new StringBuilder(100);
        String customJobName = job.getJobName();
        if (customJobName == null || customJobName.trim().isEmpty()) {
            name.append(className);
        } else {
            name.append(customJobName);
        }
        name.append('-').append(mapper.getSimpleName());
        name.append('-').append(reducer.getSimpleName());
        return name.toString();
    }


    public static void delete(Configuration conf, Iterable<Path> paths) throws IOException {
        if (conf == null) {
            conf = new Configuration();
        }
        for (Path path : paths) {
            FileSystem fs = path.getFileSystem(conf);
            if (fs.exists(path)) {
                log.info("Deleting {}", path);
                fs.delete(path, true);
            }
        }
    }

    public static void delete(Configuration conf, Path... paths) throws IOException {
        delete(conf, Arrays.asList(paths));
    }

    public static long countRecords(Path path, Configuration conf) throws IOException {
        long count = 0;
        Iterator<?> iterator = new SequenceFileValueIterator<Writable>(path, true, conf);
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

    public static long countRecords(Path path, PathType pt, PathFilter filter, Configuration conf) throws IOException {
        long count = 0;
        Iterator<?> iterator = new SequenceFileDirValueIterator<Writable>(path, pt, filter, null, true, conf);
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }
}