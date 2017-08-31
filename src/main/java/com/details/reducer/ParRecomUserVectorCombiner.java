package com.details.reducer;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 将计算部分用户推荐向量的结果进行合并，将userID对应的贡现向量的分值进行相加(注意：这个只是将一个map的输出进行合并，所以这个也是只部分结果)
 * @author 曾昭正
 */
public class ParRecomUserVectorCombiner extends Reducer<VarLongWritable, VectorWritable, VarLongWritable, VectorWritable>{
    private static final Logger logger = LoggerFactory.getLogger(ParRecomUserVectorCombiner.class);
    @Override
    protected void reduce(VarLongWritable userID, Iterable<VectorWritable> coocVectorColunms,Context context)
            throws IOException, InterruptedException {

        Vector vectorColunms = null;

        for(VectorWritable  coocVectorColunm : coocVectorColunms){
            vectorColunms = vectorColunms == null ? coocVectorColunm.get() : vectorColunms.plus(coocVectorColunm.get());
        }
        logger.info(userID +" " + new VectorWritable(vectorColunms));
        context.write(userID, new VectorWritable(vectorColunms));
    }
}