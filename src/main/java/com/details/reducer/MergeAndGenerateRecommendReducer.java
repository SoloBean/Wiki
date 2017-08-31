package com.details.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.RecommendedItemsWritable;
import org.apache.mahout.cf.taste.impl.recommender.ByValueRecommendedItemComparator;
import org.apache.mahout.cf.taste.impl.recommender.GenericRecommendedItem;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.math.VarLongWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 合并所有已经评分的共现矩阵
 * @author 曾昭正
 */
public class MergeAndGenerateRecommendReducer extends Reducer<VarLongWritable, VectorWritable, VarLongWritable, RecommendedItemsWritable>{
    private static final Logger logger = LoggerFactory.getLogger(MergeAndGenerateRecommendReducer.class);
    private int recommendationsPerUser;
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        recommendationsPerUser = context.getConfiguration().getInt("recomandItems.recommendationsPerUser", 5);
    }
    @Override
    protected void reduce(VarLongWritable userID, Iterable<VectorWritable> cooVectorColumn,Context context)
            throws IOException, InterruptedException {
        //分数求和合并
        Vector recommdVector = null;
        for(VectorWritable vector : cooVectorColumn){
            recommdVector = recommdVector == null ? vector.get() : recommdVector.plus(vector.get());
        }
        //对推荐向量进行排序，为每个UserID找出topM个推荐选项(默认找出5个)，此队列按照item对应的分数进行排序
        //注意下：PriorityQueue队列的头一定是最小的元素,另外这个队列容量增加1是为了为添加更大的新元素时使用的临时空间
        Queue<RecommendedItem> topItems = new PriorityQueue<RecommendedItem>(recommendationsPerUser+1, ByValueRecommendedItemComparator.getInstance());

        Iterator<Element> it = recommdVector.nonZeroes().iterator();
        while(it.hasNext()){
            Element e = it.next();
            int itemID = e.index();
            float preValue = (float) e.get();
            //当队列容量小于推荐个数，往队列中填item和分数
            if(topItems.size() < recommendationsPerUser){
                topItems.add(new GenericRecommendedItem(itemID, preValue));
            }
            //当前item对应的分数比队列中的item的最小分数大，则将队列头原始（即最小元素）弹出，并且将当前item：分数加入队列
            else if(preValue > topItems.peek().getValue()){
                topItems.add(new GenericRecommendedItem(itemID, preValue));
                //弹出头元素（最小元素）
                topItems.poll();
            }
        }

        //重新调整队列的元素的顺序
        List<RecommendedItem> recommdations = new ArrayList<RecommendedItem>(topItems.size());
        recommdations.addAll(topItems);//将队列中所有元素添加即将排序的集合
        Collections.sort(recommdations,ByValueRecommendedItemComparator.getInstance());//排序

        //输出推荐向量信息
        logger.info(userID+" "+ new RecommendedItemsWritable(recommdations));
        context.write(userID, new RecommendedItemsWritable(recommdations));

    }
}