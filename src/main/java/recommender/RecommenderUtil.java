package recommender;

import main.Utils;

import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;

import com.google.common.base.Optional;

public class RecommenderUtil {

	/**
	 * 1- Collect k-many items by integrating the items used by the neighbors
	 * 2- Return top-k items
	 * 
	 * NOTE: Used by UserBasedCF and MultiObjRec
	 * 
	 * @param sc
	 * @param dataFlattened: Input data with the format userid-->itemid
	 * @param neighbors: Neighbors with format userid-->neighborid
	 * @param k: output list size
	 * @return recommended items
	 */
	public static JavaPairRDD<Integer,Integer> selectItemsFromNeighbors(JavaPairRDD<Integer, Integer> dataFlattened, 
			JavaPairRDD<Integer,Integer> neighbors, int k){

		// apply join on dataMapped and neighbors: i.e (targetUser,neighbor) (neighbor,<itemList>) --> neighbor, (targetUser, <itemList>)
		JavaPairRDD<Integer,Integer> neighborsSwapped = neighbors.mapToPair((Tuple2<Integer,Integer>n)->n.swap());
		//neighborsSwapped.foreach(entry->System.out.println(entry.toString()));
		//System.out.println(neighborsSwapped.count());
		//System.out.println(dataMapped.count());


		JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> joined = dataFlattened.leftOuterJoin(neighborsSwapped);
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedMapped = joined.mapToPair(tuple-> Utils.removeOptional(tuple));
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedMappedFiltered = joinedMapped.filter(tuple-> tuple._2()._2() >= 0);

		// get the items that are suggested to target : userid--> recitemId
		JavaPairRDD<Integer,Integer> recList = joinedMappedFiltered.mapToPair(f->new Tuple2<Integer,Integer>(f._2()._2(),f._2()._1()));
		//JavaPairRDD<Integer, Iterable<Integer>> recListConcat = recList.groupByKey();
		// print
		//recListConcat.filter(x->x._1==1).foreach(tuple->Printer.printTupleWithIterable(tuple));

		// find topk
		JavaPairRDD<Integer,Integer> topKRecItems = Utils.getTopK(k, recList);
		// print
		//topKRecItems.foreach(e->System.out.println(e._1 + " , " + e._2));

		return topKRecItems;
	}

	/**
	 * 1- Collect k-many items by integrating the items identified to be neighbor item*
	 * 2- Return top-k items
	 * 
	 * *neighbor item: Similar items to the ones that are used by the target user
	 * @param dataFlattened: userid-->itemid
	 * @param k: output list size
	 * @return recommended items
	 */
	public static JavaPairRDD<Integer, Integer> selectItemsFromItems(
			JavaPairRDD<Integer, Integer> dataFlattened,
			JavaPairRDD<Integer, Integer> neighbors, int k) {
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
}
