
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;

import scala.Serializable;
import scala.Tuple2;

import com.google.common.base.Optional;

public class UserBasedCollabFiltering implements Serializable {

	private static final long serialVersionUID = -1413094281502110275L;


	public static JavaPairRDD<Integer,Integer> performCollaborativeFiltering(JavaSparkContext sc, 
			JavaPairRDD<Integer, Integer> dataFlattened, int k){

		// calculate cosine similarity of users 
		JavaRDD<Vector> vectorOfUsers = createVectorOf(dataFlattened);
		//vectorOfUsers.foreach(v->System.out.println(v.toString()));
		JavaRDD<MatrixEntry> simEntriesUnionRdd = Utils.calculateCosSim(vectorOfUsers);
		//JavaRDD<Iterable<MatrixEntry>> groupedSimUnion = simEntriesUnionRdd.groupBy(m->m.i()).values();
		// print similarities
		//groupedSimUnion.foreach(entry->print(entry));

		// Create sorted list (based on similarity) of other users for each user	
		// sort by value and group by i // TODO does this always return sorted list after groupby?
		JavaRDD<MatrixEntry> sortedSimEntriesUnionRdd = simEntriesUnionRdd.sortBy(x->x.value(),false,1);
		JavaRDD<Iterable<MatrixEntry>> groupedSortedSimUnion = sortedSimEntriesUnionRdd.groupBy(m->m.i()).values();
		//groupedSortedSimUnion.foreach(entry->print(entry));

		// Select most similar N entries 
		int N = 5;
		JavaRDD<MatrixEntry> topN = groupedSortedSimUnion.flatMap((Iterable<MatrixEntry> eList)->Utils.getTopN(N, eList));
		// print top-k
		//topK.foreach(entry->System.out.println(entry.toString()));

		// Select most similar users (i.e. neighbors)
		JavaPairRDD<Integer,Integer> neighbors = topN.mapToPair((MatrixEntry topElement)->new Tuple2<Integer,Integer>((int)topElement.i(), (int)topElement.j()));
		neighbors.cache();
		// print neighbors
		//neighbors.foreach(entry->System.out.println(entry.toString()));



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
		JavaPairRDD<Integer, Iterable<Integer>> recListConcat = recList.groupByKey();
		// print
		recListConcat.filter(x->x._1==1).foreach(tuple->Printer.printTupleWithIterable(tuple));

		// find topk
		JavaPairRDD<Integer,Integer> topKRecItems = Utils.getTopK(k, recList);
		// print
		//topKRecItems.foreach(e->System.out.println(e._1 + " , " + e._2));
		
		return topKRecItems;
	}


	/**
	 * @param data: itemId-->userId RDD
	 * @return SparseVector of user freq. for each item
	 */
	private static  JavaRDD<Vector> createVectorOf(JavaPairRDD<Integer, Integer> dataFlattened) {
		JavaRDD<Vector> retVector = null;

		// create inverted index representation: itemId to userId e.g. i21-->u3
		JavaPairRDD<Integer, Integer> invertedIndexMapped = dataFlattened.mapToPair(tuple-> tuple.swap());
		// print inverted list
		//invertedIndexMapped.foreach(t->System.out.println(t._1() + " , " + t._2()));

		int largestUserId = Utils.findLargestValueId(invertedIndexMapped);
		//System.out.println(largestUserId);


		// TODO Normally Version 1 and 2 should produce same results, but they do not!!!
		//////////Version 1
		// create itemid-->userid list
		JavaPairRDD<Integer, Iterable<Integer>> invertedIndexGrouped = invertedIndexMapped.groupByKey();
		//invertedIndexGrouped.foreach(t->printTuple2(t));

		JavaRDD<Iterable<Integer>> values = invertedIndexGrouped.values();
		// for each rdd(~entry) find the freq. of users
		retVector = values.map(uList-> Utils.countVals(largestUserId+1, uList));
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
	

}
