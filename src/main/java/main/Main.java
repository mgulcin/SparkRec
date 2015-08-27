package main;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import recommender.HybridRec;
import recommender.ItemBasedCollabFiltering;
import recommender.MultiObjectiveRec;
import recommender.UserBasedCollabFiltering;
import scala.Tuple2;
import eval.Evaluate;
import eval.EvaluationResult;

public class Main implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6106269076155338045L;
	public static String logPath;
	public static JavaSparkContext sc;

	public static void main(String[] args) {

		if(args.length != 3){
			System.out.println("Wrong number of arguments. Give a file as input.");
			System.exit(-1);
		}

		// file for log
		logPath = args[0];

		// some file in the form of userId,<comma separated list of itemIds> e.g. u1,i1,i32,i36,i94
		String trainFile = args[1];
		String testFile = args[2];

		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g");
		sc = new JavaSparkContext(conf);

		// perform recommendation
		// read data from file: userid, itemid e.g. u3-->i21,u3-->i45, u3-->i89
		JavaPairRDD<Integer, Integer> trainDataFlattened = Utils.readData(sc, trainFile);
				
		// inclusion of multiple features
		List<JavaPairRDD<Integer, Integer>> inputDataList = new ArrayList<JavaPairRDD<Integer,Integer>>();
		inputDataList.add(trainDataFlattened);

		// recommend
		int k = 3;
		int N = 2;
		JavaPairRDD<Integer, Integer> recOutput = recommendbByUserBasedCollabFiltering(N,k, trainDataFlattened);
		//JavaPairRDD<Integer, Integer> recOutput = recommendbByItemBasedCollabFiltering(N,k, trainDataFlattened);
		//JavaPairRDD<Integer, Integer> recOutput = recommendbByHybridCollabFiltering(N,k, trainDataFlattened);
		//JavaPairRDD<Integer, Integer> recOutput = recommendbByMultiObjectiveRec(N,k, inputDataList);

		// print
		//recOutput.foreach(e->Printer.printToFile(logPath, e._1 + " , " + e._2));



		// perform test
		// read data from file: userid, itemid e.g. u3-->i21,u3-->i45, u3-->i89
		JavaPairRDD<Integer, Integer> testDataFlattened = Utils.readData(sc, testFile);

		// evaluate
		EvaluationResult evalResult = Evaluate.evaluate(recOutput,testDataFlattened);
		// print
		Printer.printToFile(logPath,evalResult.toString());

		sc.close();
	}


	private static JavaPairRDD<Integer, Integer> recommendbByMultiObjectiveRec(
			int N, int k, List<JavaPairRDD<Integer, Integer>> inputDataList) {
		MultiObjectiveRec moRec = new MultiObjectiveRec(N);
		//JavaPairRDD<Integer, Integer> recOutput = moRec.performBatchRecommendation(inputDataList, k);
		// recommend to target user only 		
		// here I perform recommendation for all users - which is not necessary in real world!!
		JavaPairRDD<Integer, Integer> baseData = inputDataList.get(0);//TODO base data 
		List<Integer> targets = baseData.keys().distinct().collect();
		JavaPairRDD<Integer, Integer> recOutput = null;

		for(Integer targetUserId: targets){
			JavaPairRDD<Integer, Integer> neighbors = moRec.selectNeighbors(targetUserId, inputDataList);// can be done in batch also out of loop

			// print neighbors
			Printer.printToFile(Main.logPath, "Neighbors: ");
			neighbors.foreach(entry->Printer.printToFile(Main.logPath, entry._1 + ", " + entry._2  ));
			
			if(recOutput == null){
				recOutput = moRec.recommend(targetUserId, inputDataList, neighbors, k);	
			} else {
				JavaPairRDD<Integer, Integer> recOutputDummy = moRec.recommend(targetUserId,  inputDataList, neighbors, k);	;		
				recOutput = recOutput.union(recOutputDummy);
			}
		}

		return recOutput;
	}

	private static JavaPairRDD<Integer, Integer> recommendbByHybridCollabFiltering(
			int N, int k, JavaPairRDD<Integer, Integer> trainDataFlattened) {
		HybridRec hybrid = new HybridRec(N);
		// recommend for all users
		//JavaPairRDD<Integer, Integer> recOutput = hybrid.performBatchRecommendation(trainDataFlattened, k);

		// recommend to target user only 		
		// here I perform recommendation for all users - which is not necessary in real world!!
		List<Integer> targets = trainDataFlattened.keys().distinct().collect();
		JavaPairRDD<Integer, Integer> recOutput = null;

		for(Integer targetUserId: targets){
			if(recOutput == null){
				recOutput = hybrid.recommend(targetUserId, trainDataFlattened, k);	
			} else {
				JavaPairRDD<Integer, Integer> recOutputDummy = hybrid.recommend(targetUserId, trainDataFlattened, k);	;		
				recOutput = recOutput.union(recOutputDummy);
			}
		}
		return recOutput;
	}

	private static JavaPairRDD<Integer, Integer> recommendbByItemBasedCollabFiltering(
			int N, int k, JavaPairRDD<Integer, Integer> trainDataFlattened) {
		ItemBasedCollabFiltering icf = new ItemBasedCollabFiltering(N);

		// recommend for all users
		//JavaPairRDD<Integer, Integer> recOutput = icf.performBatchRecommendation(trainDataFlattened,k);

		// recommend to target user only 		
		// here I perform recommendation for all users - which is not necessary in real world!!
		List<Integer> targets = trainDataFlattened.keys().distinct().collect();
		JavaPairRDD<Integer, Integer> recOutput = null;

		for(Integer targetUserId: targets){
			JavaPairRDD<Integer, Integer> neighbors = icf.selectNeighbors(targetUserId, trainDataFlattened);// can be done in batch also out of loop

			/*// print neighbors
			Printer.printToFile(Main.logPath, "Neighbors: ");
			neighbors.foreach(entry->Printer.printToFile(Main.logPath, entry._1 + ", " + entry._2  ));*/

			if(recOutput == null){
				recOutput = icf.recommend(targetUserId, trainDataFlattened, neighbors, k);	
			} else {
				JavaPairRDD<Integer, Integer> recOutputDummy = icf.recommend(targetUserId, trainDataFlattened, neighbors, k);	;		
				recOutput = recOutput.union(recOutputDummy);
			}

		}
		return recOutput;
	}


	private static JavaPairRDD<Integer, Integer> recommendbByUserBasedCollabFiltering(int N, int k,
			JavaPairRDD<Integer, Integer> trainDataFlattened) {
		UserBasedCollabFiltering ucf = new UserBasedCollabFiltering(N);

		// recommend for all users
		JavaPairRDD<Integer, Integer> recOutput = ucf.performBatchRecommendation(trainDataFlattened, k);

		/*// recommend to target user only 		
		// here I perform recommendation for all users - which is not necessary in real world!!
		List<Integer> targets = trainDataFlattened.keys().distinct().collect();
		JavaPairRDD<Integer, Integer> recOutput = null;

		for(Integer targetUserId: targets){
			JavaPairRDD<Integer, Integer> neighbors = ucf.selectNeighbors(targetUserId, trainDataFlattened);// can be done in batch also out of loop

			// print neighbors
			Printer.printToFile(Main.logPath, "Neighbors: ");
			neighbors.foreach(entry->Printer.printToFile(Main.logPath, entry._1 + ", " + entry._2  ));

			if(recOutput == null){
				recOutput = ucf.recommend(targetUserId, trainDataFlattened, neighbors, k);	
			} else {
				JavaPairRDD<Integer, Integer> recOutputDummy = ucf.recommend(targetUserId, trainDataFlattened, neighbors, k);	;		
				recOutput = recOutput.union(recOutputDummy);
			}

		}*/
		
		return recOutput;
	}



}


