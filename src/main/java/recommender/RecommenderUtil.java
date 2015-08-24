package recommender;

import main.Utils;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;

import scala.Tuple2;

import com.google.common.base.Optional;

public class RecommenderUtil {

	/**
	 * 1- Collect k-many items by integrating the items used by the neighbors
	 * 2- Return top-k items
	 * 
	 * NOTE1: Used by UserBasedCF and MultiObjRec
	 * NOTE2: dataFlattened does not have to be the same as the one used when neighbors are found. 
	 * I.e. neighbors can be found in the training phase, and during the test (or real time analysis) phase
	 * different/updated data can be used
	 * 
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
		//topKRecItems.foreach(e->System.out.println(e._1 + " , " + e._2));public 

		return topKRecItems;
	}

	/**
	 * NOTE1: onlyNeighborsData does not have to be the same as the one used when neighbors are found. 
	 * I.e. neighbors can be found in the training phase, and during the test (or real time analysis) phase
	 * different/updated data can be used
	 * 
	 * @param onlyNeighborsData: Format userid-->itemid. 
	 * @param k: output list size
	 * @return top k items suggested
	 */
	public static JavaRDD<Integer> selectItemsFromNeighbors(
			JavaPairRDD<Integer, Integer> onlyNeighborsData, int k) {
		// get the items that are used/preferred by the neighbors
		JavaRDD<Integer> allItemsSuggested = onlyNeighborsData.map(neighborsItem->neighborsItem._2);

		// select top k items- based on frequency?
		JavaRDD<Integer> topKItems = Utils.getTopK(k, allItemsSuggested);

		return topKItems;
	}

	/**
	 * TODO Version1 and Version2 should produce same results, but they don't. Control this!!!
	 * @param data: itemId-->userId RDD
	 * @return SparseVector of user freq. for each item
	 */
	public static JavaRDD<Vector> createVectorOfNeighbors(JavaPairRDD<Integer, Integer> dataFlattened) {
		JavaRDD<Vector> retVector = null;

		//////////Version 1
		// create inverted index representation: itemId to userId e.g. i21-->u3
		JavaPairRDD<Integer, Integer> invertedIndexMapped = dataFlattened.mapToPair(tuple-> tuple.swap());
		// print inverted list
		//invertedIndexMapped.foreach(t->System.out.println(t._1() + " , " + t._2()));
		retVector = createVectorOfItems(invertedIndexMapped);

		//////////Version 1

		/*////////// Version 2
		// count freq. of a user for each item : (itemId, userId)-->freq
		JavaPairRDD<Tuple2<Integer,Integer>, Integer> pairs = invertedIndexMapped.mapToPair((Tuple2<Integer,Integer> t)-> new Tuple2<Tuple2<Integer,Integer>,Integer>(t,1));
		JavaPairRDD<Tuple2<Integer,Integer>, Integer> counts = pairs.reduceByKey((x,y)-> x+y); //  pairs.reduceByKey(Integer::sum);
		//counts.foreach(t->System.out.println(t._1() + " , " + t._2()));

		// create itemid-->(userid,freq) and group by itemId
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> userFreqPerItem = counts.mapToPair((Tuple2<Tuple2<Integer,Integer>, Integer> t)
				-> new Tuple2<Integer,Tuple2<Integer,Integer>>(t._1()._1(), new Tuple2<Integer,Integer>(t._1()._2(),t._2())));		
		JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> userFreqListPerItem = userFreqPerItem.groupByKey();
		//userFreqListPerItem.foreach(t->printTuple2(t));
		retVector = userFreqListPerItem.map((Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>> t)-> createVectorOf((largestUserId+1), t));

		//////////Version 2
		 */
		return retVector;
	}	


	/**
	 * 1- Collect k-many items by integrating the items identified to be neighbor item*
	 * 2- Return top-k items
	 * 
	 * *neighbor item: Similar items to the ones that are used by the target user
	 * 
	 * NOTE1: Used by ItemBasedCF
	 * NOTE2: dataFlattened does not have to be the same as the one used when neighbors are found. 
	 * I.e. neighbors can be found in the training phase, and during the test (or real time analysis) phase
	 * different/updated data can be used
	 * 
	 * @param dataFlattened: userid-->itemid
	 * @param neighbors: itemid-->similar itemid
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

	/**
	 * 1- Collect k-many items by integrating the items identified to be neighbor items*
	 * 2- Return top-k items
	 * 
	 * *neighbor items: Similar items to the ones that are used by the target user
	 * 
	 * NOTE1: Used by ItemBasedCF
	 * NOTE2: dataFlattened does not have to be the same as the one used when neighbors are found. 
	 * I.e. neighbors can be found in the training phase, and during the test (or real time analysis) phase
	 * different/updated data can be used
	 * 
	 * @param onlyNeighborItemsData: userid-->itemid
	 * @param k: output list size
	 * @return recommended items
	 */
	public static JavaRDD<Integer> selectItemsFromItems(JavaPairRDD<Integer, Integer> onlyNeighborItemsData, int k) {
		// get the items that are used/preferred by the neighbors
		JavaRDD<Integer> allItemsSuggested = onlyNeighborItemsData.map(neighborsItem->neighborsItem._2);

		// select top k items- based on frequency?
		JavaRDD<Integer> topKItems = Utils.getTopK(k, allItemsSuggested);

		return topKItems;
	}

	/**
	 * @param data: userId --> itemIdRDD
	 * @return SparseVector of user freq. for each item
	 */
	public static JavaRDD<Vector> createVectorOfItems(JavaPairRDD<Integer, Integer> dataFlattened) {
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
