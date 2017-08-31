package com.details.mapper;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 计算部分用户推荐向量
 * @author 曾昭正
 */
public class CaclPartialRecomUserVectorMapper extends Mapper<IntWritable,VectorAndPrefsWritable, VarLongWritable, VectorWritable>{
    private static final Logger logger = LoggerFactory.getLogger(CaclPartialRecomUserVectorMapper.class);
    @Override
    protected void map(IntWritable itemID, VectorAndPrefsWritable values,Context context)
            throws IOException, InterruptedException {
        Vector coocVectorColumn = values.getVector();
        List<Long> userIDs = values.getUserIDs();
        List<Float> preferenceValues = values.getValues();
        for(int i = 0; i< userIDs.size(); i++){
            long userID = userIDs.get(i);
            float preferenceValue = preferenceValues.get(i);
            logger.info("userID:" + userID);
            logger.info("preferenceValue:"+preferenceValue);
            //将共现矩阵中userID对应的列相乘，算出部分用户对应的推荐列表分数
            Vector preferenceParScores = coocVectorColumn.times(preferenceValue);
            context.write(new VarLongWritable(userID), new VectorWritable(preferenceParScores));
        }
    }
}
