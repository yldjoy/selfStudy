package spark;

import com.alibaba.fastjson.JSON;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

/**
 * Created by 16080032 on 2018/7/13.
 */
public class SparkTransformation {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkTransformation").master("local").getOrCreate();

        SparkContext sc = spark.sparkContext();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 4);

        /**** map ****/
//        rdd = rdd.map(num -> num * 2);

        /**** flatMap ****/
//        rdd = rdd.flatMap(num -> {
//            List<Integer> list = new ArrayList<Integer>();
//            for (int i = 0; i < num; i ++) {
//                list.add(i);
//            }
//            return list.iterator();
//        });

        /**** mapPartition ****/
//        rdd = rdd.mapPartitions(nums -> {
//            List<Integer> list = new ArrayList<>();
//            while(nums.hasNext()) {
//                list.add(nums.next() * 2);
//            }
//            return list.iterator();
//        });

        /**** mapPartitionsWithIndex ****/
//        rdd = rdd.mapPartitionsWithIndex((partitionIndex, partitionNums) -> {
//            List<Integer> list = new ArrayList<>();
//            while (partitionNums.hasNext()) {
//                list.add(partitionNums.next() * 2 + partitionIndex);
//            }
//            System.out.println("partitionIndex is " + partitionIndex);
//            return list.iterator();
//        }, false);

        /**** sample ****/
        rdd = rdd.sample(false,0.8d,100);


        System.out.println("rdd debug string is" + rdd.toDebugString());
        System.out.println(JSON.toJSONString(rdd.collect()));
        spark.close();
    }
}
