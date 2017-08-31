package com.details;

import java.io.IOException;
import java.net.URI;

import com.details.mapper.CaclPartialRecomUserVectorMapper;
import com.details.mapper.CombineUserVectorAndCoocMatrixMapper;
import com.details.mapper.SourceDataToItemPrefsMapper;
import com.details.mapper.UserVectorToCooccurrenceMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.VectorWritable;
import com.details.mapper.UserVecotrSplitMapper;
import com.details.reducer.CombineUserVectorAndCoocMatrixReducer;
import com.details.reducer.MergeAndGenerateRecommendReducer;
import com.details.reducer.ParRecomUserVectorCombiner;
import com.details.reducer.UserVectorToCoocurrenceReducer;
import com.details.reducer.SourceDataToUserVectorReducer;
import com.details.util.HadoopUtil;


/**
 * 组装各个作业组件，完成推荐作业
 * @author 曾昭正
 */
public class PackageRecomendJob extends Configured implements Tool{
    String[] dataSourceInputPath = {"/user/hadoop/z.zeng/distruteItemCF/dataSourceInput"};
    String[] uesrVectorOutput = {"/user/hadoop/z.zeng/distruteItemCF/uesrVectorOutput/"};
    String[] userVectorSpliltOutPut = {"/user/hadoop/z.zeng/distruteItemCF/userVectorSpliltOutPut"};
    String[] cooccurrenceMatrixOuptPath = {"/user/hadoop/z.zeng/distruteItemCF/CooccurrenceMatrixOuptPath"};
    String[] combineUserVectorAndCoocMatrixOutPutPath = {"/user/hadoop/z.zeng/distruteItemCF/combineUserVectorAndCoocMatrixOutPutPath"};
    String[] caclPartialRecomUserVectorOutPutPath = {"/user/hadoop/z.zeng/distruteItemCF/CaclPartialRecomUserVectorOutPutPath"};

    protected void setup(Configuration configuration)
            throws IOException, InterruptedException {
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://master:9000"), configuration);
        Path p1 = new Path(uesrVectorOutput[0]);
        Path p2 = new Path(userVectorSpliltOutPut[0]);
        Path p3 = new Path(cooccurrenceMatrixOuptPath[0]);
        Path p4 = new Path(combineUserVectorAndCoocMatrixOutPutPath[0]);
        Path p5 = new Path(caclPartialRecomUserVectorOutPutPath[0]);

        if (hdfs.exists(p1)) {
            hdfs.delete(p1, true);
        }
        if (hdfs.exists(p2)) {
            hdfs.delete(p2, true);
        }
        if (hdfs.exists(p3)) {
            hdfs.delete(p3, true);
        }
        if (hdfs.exists(p4)) {
            hdfs.delete(p4, true);
        }
        if (hdfs.exists(p5)) {
            hdfs.delete(p5, true);
        }
    }
    //@Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf(); //获得配置文件对象
        setup(conf);
        //	DistributedCache.addArchiveToClassPath(new Path("/user/hadoop/z.zeng/distruteItemCF/lib"), conf);
        //配置计算用户向量作业
        Job wikipediaToItemPrefsJob = HadoopUtil.prepareJob(
                "WikipediaToItemPrefsJob",
                dataSourceInputPath,
                uesrVectorOutput[0],
                TextInputFormat.class,
                SourceDataToItemPrefsMapper.class,
                VarLongWritable.class,
                VarLongWritable.class,
                SourceDataToUserVectorReducer.class,
                VarLongWritable.class,
                VectorWritable.class,
                SequenceFileOutputFormat.class,
                conf);
        //配置计算共现向量作业
        Job userVectorToCooccurrenceJob = HadoopUtil.prepareJob(
                "UserVectorToCooccurrenceJob",
                uesrVectorOutput,
                cooccurrenceMatrixOuptPath[0],
                SequenceFileInputFormat.class,
                UserVectorToCooccurrenceMapper.class,
                IntWritable.class,
                IntWritable.class,
                UserVectorToCoocurrenceReducer.class,
                IntWritable.class,
                VectorOrPrefWritable.class,
                SequenceFileOutputFormat.class,
                conf);
        //配置分割用户向量作业
        Job userVecotrSplitJob = HadoopUtil.prepareJob(
                "userVecotrSplitJob",
                uesrVectorOutput,
                userVectorSpliltOutPut[0],
                SequenceFileInputFormat.class,
                UserVecotrSplitMapper.class,
                IntWritable.class,
                VectorOrPrefWritable.class,
                SequenceFileOutputFormat.class,
                conf);
        //合并共现向量和分割之后的用户向量作业
        //这个主意要将分割用户向量和共现向量的输出结果一起作为输入
        String[] combineUserVectorAndCoocMatrixIutPutPath = {cooccurrenceMatrixOuptPath[0],userVectorSpliltOutPut[0]};
        Job combineUserVectorAndCoocMatrixJob = HadoopUtil.prepareJob(
                "combineUserVectorAndCoocMatrixJob",
                combineUserVectorAndCoocMatrixIutPutPath,
                combineUserVectorAndCoocMatrixOutPutPath[0],
                SequenceFileInputFormat.class,
                CombineUserVectorAndCoocMatrixMapper.class,
                IntWritable.class,
                VectorOrPrefWritable.class,
                CombineUserVectorAndCoocMatrixReducer.class,
                IntWritable.class,
                VectorAndPrefsWritable.class,
                SequenceFileOutputFormat.class,
                conf);
        //计算用户推荐向量
        Job caclPartialRecomUserVectorJob= HadoopUtil.prepareJob(
                "caclPartialRecomUserVectorJob",
                combineUserVectorAndCoocMatrixOutPutPath,
                caclPartialRecomUserVectorOutPutPath[0],
                SequenceFileInputFormat.class,
                CaclPartialRecomUserVectorMapper.class,
                VarLongWritable.class,
                VectorWritable.class,
                ParRecomUserVectorCombiner.class,//为map设置combiner减少网络IO
                MergeAndGenerateRecommendReducer.class,
                VarLongWritable.class,
                RecommendedItemsWritable.class,
                TextOutputFormat.class,
                conf);

        //串联各个job
        if(wikipediaToItemPrefsJob.waitForCompletion(true)){
            if(userVectorToCooccurrenceJob.waitForCompletion(true)){
                if(userVecotrSplitJob.waitForCompletion(true)){
                    if(combineUserVectorAndCoocMatrixJob.waitForCompletion(true)){
                        int rs = caclPartialRecomUserVectorJob.waitForCompletion(true) ? 1 :0;
                        return rs;
                    }else{
                        throw new Exception("合并共现向量和分割之后的用户向量作业失败！！");
                    }
                }else{
                    throw new Exception("分割用户向量作业失败！！");
                }
            }else{
                throw new Exception("计算共现向量作业失败！！");
            }
        }else{
            throw new Exception("计算用户向量作业失败！！");
        }
    }
    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        try {
            int returnCode =  ToolRunner.run(new PackageRecomendJob(),args);
            System.exit(returnCode);
        } catch (Exception e) {
        }
    }

}