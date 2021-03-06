package recommender;

import main.Utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Serializable;



public class HybridRec implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private UserBasedCollabFiltering ucf;
	private ItemBasedCollabFiltering icf;




	public HybridRec(int N) {
		super();
		ucf = new UserBasedCollabFiltering(N);
		icf = new ItemBasedCollabFiltering(N);

	}


	/**
	 * 1- Get recommendations using user-based cf
	 * 2- Get recommendations using item-based cf
	 * 3- Combine recommendations from step-1 and step-2
	 * 4- Return top-k items 
	 * @param sc
	 * @param dataFlattened: input data, e.g. userid--> itemId 
	 * @param k: outputList size
	 * @return recommended items, e.g. userid--> itemId 
	 */
	public JavaPairRDD<Integer, Integer> performBatchRecommendation(JavaPairRDD<Integer, Integer> inputData, int k){

		// get the output of each rec. method: userid-->rec. itemid
		JavaPairRDD<Integer, Integer> ucfRecs = ucf.performBatchRecommendation(inputData, k);
		JavaPairRDD<Integer, Integer> icfRecs = icf.performBatchRecommendation(inputData, k);

		// combine the outputs
		JavaPairRDD<Integer, Integer> combinedOutputs = ucfRecs.union(icfRecs);
		JavaPairRDD<Integer,Integer> topKRecItems = Utils.getTopK(k, combinedOutputs);
		// print
		//topKRecItems.foreach(e->System.out.println(e._1 + " , " + e._2));


		return topKRecItems ;
	}

	public JavaPairRDD<Integer,Integer> recommend(Integer targetUserId, 
			JavaPairRDD<Integer, Integer> inputData, int k){
		
		// get the output of each rec. method: userid-->rec. itemid
		JavaPairRDD<Integer, Integer> ucfNeighbors = ucf.selectNeighbors(targetUserId, inputData);
		JavaPairRDD<Integer, Integer> ucfRecs = ucf.recommend(targetUserId, inputData, ucfNeighbors, k);
		
		JavaPairRDD<Integer, Integer> icfNeighbors = icf.selectNeighbors(targetUserId, inputData);
		JavaPairRDD<Integer, Integer> icfRecs = icf.recommend(targetUserId, inputData, icfNeighbors, k);

		// combine the outputs
		JavaPairRDD<Integer, Integer> combinedOutputs = ucfRecs.union(icfRecs);
		JavaPairRDD<Integer,Integer> topKRecItems = Utils.getTopK(k, combinedOutputs);
		// print
		//topKRecItems.foreach(e->System.out.println(e._1 + " , " + e._2));


		return topKRecItems ;
	}

}