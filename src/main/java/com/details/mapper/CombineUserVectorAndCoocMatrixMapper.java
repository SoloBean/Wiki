package com.details.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;

/**
 * 将共现矩阵和分割后的用户向量进行合并，以便计算部分的推荐向量
 * 这个mapper其实没有什么逻辑处理功能，只是将数据按照输入格式输出
 * 注意：这里的mapper输入为共现矩阵和分割后的用户向量计算过程中的共同输出的2个目录
 * mapper输入：<itemID,Vecotor<userID,preferenceValuce>> or <itemID,Vecotor<coocItemID,coocTimes>>
 * mapper输出:<itemID,Vecotor<userID,preferenceValuce>/Vecotor<coocItemID,coocTimes>>
 * @author 曾昭正
 */
public class CombineUserVectorAndCoocMatrixMapper extends Mapper<IntWritable, VectorOrPrefWritable, IntWritable, VectorOrPrefWritable>{
    @Override
    protected void map(IntWritable itemID, VectorOrPrefWritable value,Context context)
            throws IOException, InterruptedException {
        context.write(itemID, value);
    }

}