
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Serializable;


public class HybridRec implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static JavaPairRDD<Integer, Integer> performCollaborativeFiltering(JavaSparkContext sc,
			JavaPairRDD<Integer, Integer> dataFlattened, int k){
		
		// get the output of each rec. method: userid-->rec. itemid
		JavaPairRDD<Integer, Integer> ucf = UserBasedCollabFiltering.performCollaborativeFiltering(sc, dataFlattened, k);
		JavaPairRDD<Integer, Integer> icf = ItemBasedCollabFiltering.performCollaborativeFiltering(sc, dataFlattened, k);

		// combine the outputs
		JavaPairRDD<Integer, Integer> combinedOutputs = ucf.union(icf);
		JavaPairRDD<Integer,Integer> topKRecItems = Utils.getTopK(k, combinedOutputs);
		// print
		topKRecItems.foreach(e->System.out.println(e._1 + " , " + e._2));
				
		
		return topKRecItems ;
	}
	
}