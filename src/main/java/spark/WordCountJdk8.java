package spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCountJdk8 {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("WordCountJdk8").master("local")
                .getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        JavaRDD<String> lines = sc.textFile("/Users/youliangdong/Documents/code/file/sparkWordCount.txt");
        List<Tuple2<String, Integer>> wordCountMap =
                lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(x -> new Tuple2<>(x, 1))
                .reduceByKey((x, y) -> (x + y))
                .collect();

        wordCountMap.forEach(tuple -> System.out.print(tuple._1 + " : " + tuple._2 + "; "));

        sc.close();
    }
}
