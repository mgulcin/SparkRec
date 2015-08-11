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
	public static JavaPairRDD<Integer, Integer> performRecommendation(JavaSparkContext sc,
			JavaPairRDD<Integer, Integer> inputData, int k){
		
		// get the output of each rec. method: userid-->rec. itemid
		JavaPairRDD<Integer, Integer> ucf = UserBasedCollabFiltering.performRecommendation(sc, inputData, k);
		JavaPairRDD<Integer, Integer> icf = ItemBasedCollabFiltering.performRecommendation(sc, inputData, k);

		// combine the outputs
		JavaPairRDD<Integer, Integer> combinedOutputs = ucf.union(icf);
		JavaPairRDD<Integer,Integer> topKRecItems = Utils.getTopK(k, combinedOutputs);
		// print
		//topKRecItems.foreach(e->System.out.println(e._1 + " , " + e._2));
				
		
		return topKRecItems ;
	}
	
}