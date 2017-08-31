package com.details.mapper;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.item.VectorOrPrefWritable;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 将用户向量分割，以便和物品的共现向量进行合并
 * mapper输入:<userID,Vector<itemIDIndex,preferenceValuce>....>
 * reducer输出:<itemID,Vecotor<userID,preferenceValuce>....>
 * @author 曾昭正
 */
public class UserVecotrSplitMapper extends Mapper<VarLongWritable, VectorWritable, IntWritable, VectorOrPrefWritable>{
    private static final Logger logger = LoggerFactory.getLogger(UserVecotrSplitMapper.class);
    @Override
    protected void map(VarLongWritable userIDWritable, VectorWritable value,Context context)
            throws IOException, InterruptedException {
        IntWritable itemIDIndex = new IntWritable();
        long userID = userIDWritable.get();
        Vector userVector = value.get();
        Iterator<Element> it = userVector.nonZeroes().iterator();//只取非空用户向量
        while(it.hasNext()){
            Element e = it.next();
            int itemID = e.index();
            itemIDIndex.set(itemID);
            float preferenceValuce = (float) e.get();
            logger.info(itemIDIndex +" "+new VectorOrPrefWritable(userID,preferenceValuce));
            context.write(itemIDIndex, new VectorOrPrefWritable(userID,preferenceValuce));
        }

    }
}