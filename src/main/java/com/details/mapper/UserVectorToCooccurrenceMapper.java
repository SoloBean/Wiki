package com.details.mapper;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * mapper输入：<userID,VecotrWriable<index=itemID,valuce=pres>....>
 * mapper输出:<itemID,itemID>(共现物品id对)
 * @author 曾昭正
 */
public class UserVectorToCooccurrenceMapper extends Mapper<VarLongWritable, VectorWritable, IntWritable, IntWritable>{
    @Override
    protected void map(VarLongWritable userID, VectorWritable userVector,Context context)
            throws IOException, InterruptedException {
        Iterator<Vector.Element> it = userVector.get().nonZeroes().iterator();//过滤掉非空元素
        while(it.hasNext()){
            int index1 = it.next().index();
            Iterator<Vector.Element> it2 = userVector.get().nonZeroes().iterator();
            while(it2.hasNext()){
                int index2  = it2.next().index();
                context.write(new IntWritable(index1), new IntWritable(index2));
            }
        }

    }
}