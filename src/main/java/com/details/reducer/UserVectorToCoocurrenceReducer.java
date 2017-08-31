package com.details.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * reducer输入:<itemID,Iterable<itemIDs>>
 * reducer输出:<mainItemID,Vector<coocItemID,coocTime(共现次数)>....>
 * @author 曾昭正
 */
public class UserVectorToCoocurrenceReducer extends Reducer<IntWritable, IntWritable, IntWritable, VectorOrPrefWritable>{
    private static final Logger logger = LoggerFactory.getLogger(UserVectorToCoocurrenceReducer.class);
    @Override
    protected void reduce(IntWritable mainItemID, Iterable<IntWritable> coocItemIDs,Context context)
            throws IOException, InterruptedException {
        Vector coocItemIDVectorRow = new RandomAccessSparseVector(Integer.MAX_VALUE,100);
        for(IntWritable coocItem : coocItemIDs){
            int itemCoocTime = coocItem.get();
            coocItemIDVectorRow.set(itemCoocTime,coocItemIDVectorRow.get(itemCoocTime)+1.0);//将共现次数累加
        }
        logger.info(mainItemID +" "+new VectorOrPrefWritable(coocItemIDVectorRow));
        context.write(mainItemID, new VectorOrPrefWritable(coocItemIDVectorRow));//记录mainItemID的完整共现关系
    }
}