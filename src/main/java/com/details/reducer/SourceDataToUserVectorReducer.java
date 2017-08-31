package com.details.reducer;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * reducer输入：<userID,Iterable<itemID>>
 * reducer输出:<userID,VecotrWriable<index=itemID,valuce=pres>....>
 * @author 曾昭正
 */
public class SourceDataToUserVectorReducer extends Reducer<VarLongWritable, VarLongWritable, VarLongWritable, VectorWritable>{
    private static final Logger logger = LoggerFactory.getLogger(SourceDataToUserVectorReducer.class);
    @Override
    protected void reduce(VarLongWritable userID, Iterable<VarLongWritable> itemPrefs,Context context)
            throws IOException, InterruptedException {
        /**
         *  DenseVector，它的实现就是一个浮点数数组，对向量里所有域都进行存储，适合用于存储密集向量。
         RandomAccessSparseVector 基于浮点数的 HashMap 实现的，key 是整形 (int) 类型，value 是浮点数 (double) 类型，它只存储向量中不为空的值，并提供随机访问。
         SequentialAccessVector 实现为整形 (int) 类型和浮点数 (double) 类型的并行数组，它也只存储向量中不为空的值，但只提供顺序访问。
         用户可以根据自己算法的需求选择合适的向量实现类，如果算法需要很多随机访问，应该选择 DenseVector 或者 RandomAccessSparseVector，如果大部分都是顺序访问，SequentialAccessVector 的效果应该更好。
         介绍了向量的实现，下面我们看看如何将现有的数据建模成向量，术语就是“如何对数据进行向量化”，以便采用 Mahout 的各种高效的聚类算法。
         */
        Vector userVector = new RandomAccessSparseVector(Integer.MAX_VALUE, 100);
        for(VarLongWritable itemPref : itemPrefs){
            userVector.set((int)itemPref.get(), 1.0f);//RandomAccessSparseVector.set(index,value),用户偏好类型为boolean类型，将偏好值默认都为1.0f
        }
        logger.info(userID+" "+new VectorWritable(userVector));
        context.write(userID, new VectorWritable(userVector));
    }
}