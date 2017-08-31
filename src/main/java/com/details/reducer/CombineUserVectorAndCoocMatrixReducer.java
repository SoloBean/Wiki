package com.details.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.item.VectorAndPrefsWritable;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 将共现矩阵和分割后的用户向量进行合并，以便计算部分的推荐向量
 * @author 曾昭正
 */
public class CombineUserVectorAndCoocMatrixReducer extends Reducer<IntWritable, VectorOrPrefWritable, IntWritable, VectorAndPrefsWritable>{
    private static final Logger logger = LoggerFactory.getLogger(CombineUserVectorAndCoocMatrixReducer.class);
    @Override
    protected void reduce(IntWritable itemID, Iterable<VectorOrPrefWritable> values,Context context)
            throws IOException, InterruptedException {
        VectorAndPrefsWritable vectorAndPrefsWritable = new VectorAndPrefsWritable();
        List<Long> userIDs = new ArrayList<Long>();
        List<Float> preferenceValues = new ArrayList<Float>();
        Vector coocVector = null;
        Vector coocVectorTemp = null;
        Iterator<VectorOrPrefWritable> it = values.iterator();
        while(it.hasNext()){
            VectorOrPrefWritable e = it.next();
            coocVectorTemp = e.getVector() ;
            if(coocVectorTemp == null){
                userIDs.add(e.getUserID());
                preferenceValues.add(e.getValue());
            }else{
                coocVector = coocVectorTemp;
            }
        }
        if(coocVector != null){
            //这个需要注意，根据共现矩阵的计算reduce聚合之后，到了这个一个Reudce分组就有且只有一个vecotr(即共现矩阵的一列或者一行，这里行和列是一样的)了。
            vectorAndPrefsWritable.set(coocVector, userIDs, preferenceValues);
            logger.info(itemID+" "+vectorAndPrefsWritable);
            context.write(itemID, vectorAndPrefsWritable);
        }
    }
}