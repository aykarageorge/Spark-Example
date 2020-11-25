package sparkbasics;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class WordCount{

	public static void wordCount(String filename) {
		SparkConf config = new SparkConf().setMaster("local").setAppName("Word Counter");
		JavaSparkContext context = new JavaSparkContext(config);
		JavaRDD<String> inputFile = context.textFile(filename);
		
		JavaRDD<String> wordFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
		JavaPairRDD countData = wordFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int)x + (int) y);
		countData.saveAsTextFile("outputfile1");

	}
	
	public static void main(String args[]) {
		if(args.length == 0) {
			System.out.println("enter source file address");
			System.exit(0);
		}
		wordCount(args[0]);
	}

}
