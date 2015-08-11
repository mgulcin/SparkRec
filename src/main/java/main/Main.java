package main;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import recommender.HybridRec;
import scala.Tuple2;
import eval.Evaluate;
import eval.EvaluationResult;

public class Main implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6106269076155338045L;

	public static void main(String[] args) {

		if(args.length != 2){
			System.out.println("Wrong number of arguments. Give a file as input.");
			System.exit(-1);
		}

		// some file in the form of userId,<comma separated list of itemIds> e.g. u1,i1,i32,i36,i94
		String trainFile = args[0];
		String testFile = args[1];

		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// perform recommendation
		// read data from file: userid, itemid e.g. u3-->i21,u3-->i45, u3-->i89
		JavaPairRDD<Integer, Integer> trainDataFlattened = readData(sc, trainFile);

		// recommend
		int k = 5;
		//JavaPairRDD<Integer, Integer> recOutput = UserBasedCollabFiltering.performCollaborativeFiltering(sc, trainDataFlattened, k);
		//JavaPairRDD<Integer, Integer> recOutput = ItemBasedCollabFiltering.performCollaborativeFiltering(sc, trainDataFlattened,k);
		JavaPairRDD<Integer, Integer> recOutput = HybridRec.performRecommendation(sc, trainDataFlattened, k);
		// print
		//recOutput.foreach(e->System.out.println(e._1 + " , " + e._2));

		// perform test
		// read data from file: userid, itemid e.g. u3-->i21,u3-->i45, u3-->i89
		JavaPairRDD<Integer, Integer> testDataFlattened = readData(sc, testFile);
		
		// evaluate
		EvaluationResult evalResult = Evaluate.evaluate(recOutput,testDataFlattened);
		// print
		System.out.println(evalResult.toString());
		
		sc.close();
	}

	/**
	 * 
	 * @param sc: JavaSparkContext
	 * @param file: File to be read with format: userid, itemid1, itemid2, ...
	 * @return flattenedData: userid, itemid e.g. u3-->i21
	 */
	private static JavaPairRDD<Integer, Integer>  readData(JavaSparkContext sc, String file) {
		// load data : userid, itemid1,itemid2,...
		JavaRDD<String> data = sc.textFile(file);

		// parse data: userid, <list of itemid> e.g. u3--><i21,i45,i89>
		JavaRDD<ArrayList<Integer>> dataSplitted = data.map((String line)->splitLine(line));
		JavaPairRDD<Integer,Iterable<Integer>> dataMapped = dataSplitted.mapToPair((ArrayList<Integer> userItemList)->new Tuple2<Integer, Iterable<Integer>>(userItemList.get(0), 
				new ArrayList<Integer>(userItemList.subList(1,userItemList.size()))));
		// print 
		//dataMapped.foreach(s->System.out.println(s));
		JavaPairRDD<Integer, Iterable<Integer>> dataMappedFiltered = dataMapped.filter(dm->(((Collection<Integer>) dm._2()).size() > 0));
		// print 
		//dataMappedFiltered.foreach(s->System.out.println(s));

		// flatten dataMapped: userid, itemid e.g. u3-->i21,u3-->i45, u3-->i89
		JavaPairRDD<Integer, Integer> dataFlattened = dataMappedFiltered.flatMapValues(e->e);
		dataFlattened.cache();
		// print
		//dataFlattened.foreach(s->System.out.println(s));
		
		return dataFlattened;
	}
	
	/**
	 * 
	 * @param line: userid, itemid1, itemid2, ...
	 * @return
	 */
	private static ArrayList<Integer> splitLine(String line) {
		String[] splitted = line.split(",");
		ArrayList<Integer> intVals = new ArrayList<Integer>(splitted.length);
		for(String s: splitted){
			intVals.add(Integer.valueOf(s));
		}

		return intVals;
	}

}


