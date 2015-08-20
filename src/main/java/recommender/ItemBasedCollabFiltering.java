package recommender;

import main.Utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;

import scala.Serializable;
import scala.Tuple2;

import com.google.common.base.Optional;


public class ItemBasedCollabFiltering implements Serializable {

	private static final long serialVersionUID = -2661482178306286702L;

	// number of most similar entries(items)
	private int N;


	public ItemBasedCollabFiltering(int n) {
		super();
		N = n;
	}

	public int getN() {
		return N;
	}


	public void setN(int n) {
		N = n;
	}

	/**
	 * 1- Calculate similarity among items
	 * 2- Collect N-similar items to the ones that are used by the target user
	 * 3- Collect k-many items by integrating the items identified in step-2 
	 * 4- Return top-k items
	 * @param sc
	 * @param dataFlattened: userid-->itemid
	 * @param k: output list size
	 * @return recommended items
	 */
	public JavaPairRDD<Integer,Integer> performRecommendation(JavaSparkContext sc, 
			JavaPairRDD<Integer, Integer> dataFlattened, int k){

		// Select most similar users (i.e. neighbors)
		JavaPairRDD<Integer,Integer> neighbors = selectNeighbors(dataFlattened);
		neighbors.cache();
		// print neighbors
		//neighbors.foreach(entry->System.out.println(entry.toString()));

		// find topk
		JavaPairRDD<Integer,Integer> topKRecItems = RecommenderUtil.selectItemsFromItems(dataFlattened, neighbors, k);
		// print
		//topKRecItems.foreach(e->System.out.println(e._1 + " , " + e._2));

		return topKRecItems;
	}

	/**
	 * 1- Calculate similarity among items
	 * 2- Collect N-similar items to the ones that are used by the target user
	 * @param dataFlattened: userid-->itemid
	 * @return N similar items
	 */
	private JavaPairRDD<Integer, Integer> selectNeighbors(
			JavaPairRDD<Integer, Integer> dataFlattened) {
		// calculate cosine similarity of items
		JavaRDD<Vector> vectorOfUsers = createVectorOf(dataFlattened);
		// print
		//vectorOfUsers.rdd().toJavaRDD().foreach(v->System.out.println(v.toString()));
		JavaRDD<MatrixEntry> simEntriesUnionRdd = Utils.calculateCosSim(vectorOfUsers);

		// Create sorted list (based on similarity) of other items	
		// sort by value and group by i // TODO does this always return sorted list after groupby?
		JavaRDD<MatrixEntry> sortedSimEntriesUnionRdd = simEntriesUnionRdd.sortBy(x->x.value(),false,1);
		JavaRDD<Iterable<MatrixEntry>> groupedSortedSimUnion = sortedSimEntriesUnionRdd.groupBy(m->m.i()).values();
		//groupedSortedSimUnion.foreach(entry->print(entry));

		// Select most similar N entries(items) 
		JavaRDD<MatrixEntry> topN = groupedSortedSimUnion.flatMap((Iterable<MatrixEntry> eList)->Utils.getTopN(N, eList));
		// print top-k
		//topN.filter(m->m.i()==85).foreach(entry->System.out.println(entry.toString()));

		// Select most similar items (i.e. neighbors)
		JavaPairRDD<Integer,Integer> neighbors = topN.mapToPair((MatrixEntry topElement)->new Tuple2<Integer,Integer>((int)topElement.i(), (int)topElement.j()));

		return neighbors;
	}

	/**
	 * @param data: userId --> itemIdRDD
	 * @return SparseVector of user freq. for each item
	 */
	private JavaRDD<Vector> createVectorOf(JavaPairRDD<Integer, Integer> dataFlattened) {
		JavaRDD<Vector> retVector = null;

		int largestValId = Utils.findLargestValueId(dataFlattened);
		//System.out.println(largestValId);

		// create userid-->itemid list
		JavaPairRDD<Integer, Iterable<Integer>> indexGrouped = dataFlattened.groupByKey();
		//indexGrouped.foreach(t->printTuple2(t));

		JavaRDD<Iterable<Integer>> values = indexGrouped.values();
		// for each rdd(~entry) find the freq. of items
		retVector = values.map(valList-> Utils.countVals(largestValId+1, valList));


		return retVector;
	}


}
