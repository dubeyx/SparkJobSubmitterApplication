package com.courpedia.SparkJobSubmitter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import scala.Tuple2;
import java.util.Arrays;

@SpringBootApplication
public class SparkJobSubmitterApplication {
private static void wordCount(){
try {
	SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("WordCounter");
	JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
	JavaRDD<String> inputFile = sparkContext.textFile("words.txt");
	JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
	JavaPairRDD counData = wordsFromFile.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);
	counData.saveAsTextFile("outPutPath");
}
catch (Exception e){
	System.out.println("Error Occured"+ e);
}

}
	public static void main(String[] args) {
		SpringApplication.run(SparkJobSubmitterApplication.class, args);
		wordCount();
	}

}
