package main;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import com.google.common.base.Optional;

public class SparkSampleMain {
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g");;
		JavaSparkContext sc = new JavaSparkContext(conf);

		/* String file = "C:\\Users\\makbule.ozsoy\\Desktop\\temp.txt"; 
	    JavaRDD<String> data = sc.textFile(file).cache();

	    long numWord3s = data.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("word3"); }
	    }).count();

	    long numWord1s = data.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("word1"); }
	    }).count();

	    System.out.println("Lines with word3: " + numWord3s + ", lines with word1: " + numWord1s);*/


		/*  List<Integer> intdata = Arrays.asList(1, 2, 3, 4, 5);
	    JavaRDD<Integer> distData = sc.parallelize(intdata);

	    Integer result = distData.reduce((a, b) -> a + b);
	    System.out.println("Sum: "+result);*/

		List<Tuple2<String,String>> ndata = new ArrayList<>();
		ndata.add(new Tuple2("u1","u1"));
		ndata.add(new Tuple2("u1","u2"));
		ndata.add(new Tuple2("u2","u3"));
		ndata.add(new Tuple2("u3","u4"));
		ndata.add(new Tuple2("u4","u5"));
		JavaRDD<Tuple2<String,String>> distNData = sc.parallelize(ndata);

		List<Tuple2<String,Iterable<String>>> idata = new ArrayList<>();
		List<String> itemList = new ArrayList<>();
		itemList.add("i6");
		itemList.add("i7");
		idata.add(new Tuple2("u1",itemList));
		idata.add(new Tuple2("u2",itemList));
		JavaRDD<Tuple2<String,Iterable<String>>> distIData = sc.parallelize(idata);

		JavaPairRDD<String,  String> pairsNData = JavaPairRDD.fromJavaRDD(distNData);
		JavaPairRDD<String,  Iterable<String>> pairsIData = JavaPairRDD.fromJavaRDD(distIData);

		JavaPairRDD<String, Tuple2<Iterable<String>, Optional<String>>> joined = pairsIData.leftOuterJoin(pairsNData);
		List<Tuple2<String, Tuple2<Iterable<String>, Optional<String>>>> collected = joined.collect();
		for(Tuple2<String, Tuple2<Iterable<String>, Optional<String>>> tuple: collected){
			System.out.print(tuple._1() + " , ");
			Tuple2<Iterable<String>, Optional<String>> t2 = tuple._2();

			for(String s: t2._1()){
				System.out.print(s+ " , ");
			}

			if(t2._2().isPresent()){
				System.out.print(t2._2().get()+ " , ");
			}
			System.out.println();
		}

	}
}
