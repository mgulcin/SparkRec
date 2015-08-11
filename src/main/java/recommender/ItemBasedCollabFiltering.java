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
	private static int N = 5;
	

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
	public static JavaPairRDD<Integer,Integer> performRecommendation(JavaSparkContext sc, 
			JavaPairRDD<Integer, Integer> dataFlattened, int k){
		
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
		neighbors.cache();
		// print neighbors
		//neighbors.filter(entry->entry._1==85).foreach(entry->System.out.println(entry.toString()));

		// suggest similar items (neighbor items) to the target user
		// apply join on dataFlattened and neighbors: i.e (targetUser,item) (item, itemNeighbor)--> item, (targetUser, itemNeighbor)

		JavaPairRDD<Integer,Integer> dataFlattenedSwapped = dataFlattened.mapToPair((Tuple2<Integer,Integer>n)->n.swap());
		JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> joined = dataFlattenedSwapped.leftOuterJoin(neighbors);
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedMapped = joined.mapToPair(tuple-> Utils.removeOptional(tuple));
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedMappedFiltered = joinedMapped.filter(tuple-> tuple._2()._2() >= 0);

		// get the items that are suggested to target : userid--> recitemId
		JavaPairRDD<Integer,Integer> recList = joinedMappedFiltered.mapToPair(f->f._2());
		//JavaPairRDD<Integer, Iterable<Integer>> recListConcat = recList.groupByKey();
		// print
		//recListConcat.filter(tuple->tuple._1==5).foreach(tuple->printTupleWithIterable(tuple));

		// find topk recItems per user
		JavaPairRDD<Integer,Integer> topKRecItems = Utils.getTopK(k, recList);
		// print
		//topKRecItems.foreach(e->System.out.println(e._1 + " , " + e._2));
		
		return topKRecItems;
	}

	
	/**
	 * @param data: userId --> itemIdRDD
	 * @return SparseVector of user freq. for each item
	 */
	private static  JavaRDD<Vector> createVectorOf(JavaPairRDD<Integer, Integer> dataFlattened) {
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
	
	public static int getN() {
		return N;
	}


	public static void setN(int n) {
		N = n;
	}


}
