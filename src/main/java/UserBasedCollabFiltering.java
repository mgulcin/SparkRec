
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

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

		RowMatrix matrix = new RowMatrix(vectorOfUsers.rdd());
		// NOTE result is listed for upper triangle, i.e. for j indices larger than i indices
		// -- i.e. An n x n sparse upper-triangular matrix of cosine similarities between columns of this matrix.
		CoordinateMatrix simsPerfect = matrix.columnSimilarities();//TODO write your own cosine sim to learn

		// create full similarity matrix (from upper triangular)
		JavaRDD<MatrixEntry> simEntriesUnionRdd = createFullMatrix(simsPerfect);
		//JavaRDD<Iterable<MatrixEntry>> groupedSimUnion = simEntriesUnionRdd.groupBy(m->m.i()).values();
		// print similarities
		//groupedSimUnion.foreach(entry->print(entry));

		// Create sorted list (based on similarity) of other users for each user	
		// sort by value and group by i // TODO does this always return sorted list after groupby?
		JavaRDD<MatrixEntry> sortedSimEntriesUnionRdd = simEntriesUnionRdd.sortBy(x->x.value(),false,1);
		JavaRDD<Iterable<MatrixEntry>> groupedSortedSimUnion = sortedSimEntriesUnionRdd.groupBy(m->m.i()).values();
		//groupedSortedSimUnion.foreach(entry->print(entry));

		// Select most similar N entries 
		int N = 2;
		JavaRDD<MatrixEntry> topN = groupedSortedSimUnion.flatMap((Iterable<MatrixEntry> eList)->getTopN(N, eList));
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
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedMapped = joined.mapToPair(tuple-> removeOptional(tuple));
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedMappedFiltered = joinedMapped.filter(tuple-> tuple._2()._2() >= 0);

		// get the items that are suggested to target : userid--> recitemId
		JavaPairRDD<Integer,Integer> recList = joinedMappedFiltered.mapToPair(f->new Tuple2<Integer,Integer>(f._2()._2(),f._2()._1()));
		//JavaPairRDD<Integer, Iterable<Integer>> recListConcat = recList.groupByKey();
		// print
		//recListConcat.foreach(tuple->printTuple2(tuple));

		// find topk
		JavaPairRDD<Integer,Integer> topKRecItems = Utils.getTopK(k, recList);
		// print
		topKRecItems.foreach(e->System.out.println(e._1 + " , " + e._2));
		
		return topKRecItems;
	}


	private static Tuple2<Integer,Iterable<Integer>> selectItems(
			JavaPairRDD<Integer, Iterable<Integer>> dataMappedFiltered,
			Integer nId, Integer tuId) {

		Iterable<Integer> recItems = selectItems(dataMappedFiltered,nId);
		return new Tuple2<Integer,Iterable<Integer>>(tuId,recItems);
	}





	private static List<Integer> selectItems(
			JavaPairRDD<Integer, Iterable<Integer>> dataMappedFiltered,
			Integer nId) {
		List<Integer> resVal = dataMappedFiltered.filter((dm->dm._1 == nId)).flatMap(dm->dm._2).collect();
		return resVal;
	}





	private boolean isElementOf(Tuple2<Integer, Iterable<Integer>> dm,
			JavaPairRDD<Integer, Integer> neighborsSwapped) {
		boolean retVal = false;
		JavaRDD<Boolean> temp = neighborsSwapped.map(n-> n._1.equals(dm._1())).filter(r->r==true);
		long trueCount = temp.count();
		if(trueCount > 0){
			retVal = true;
		}
		return retVal;
	}

	private static Tuple2<Integer, Tuple2<Integer, Integer>> removeOptional(Tuple2<Integer, Tuple2<Integer, Optional<Integer>>> tuple)
	{
		if(tuple._2()._2().isPresent()){
			return new Tuple2<Integer, Tuple2<Integer, Integer>>(tuple._1(), new Tuple2<Integer, Integer>(tuple._2()._1(), tuple._2()._2().get()));
		}else{
			return new Tuple2<Integer, Tuple2<Integer, Integer>>(tuple._1(), new Tuple2<Integer, Integer>(tuple._2()._1(), -1));
		}
	};


	private static  JavaRDD<MatrixEntry> createFullMatrix(
			CoordinateMatrix simsPerfect) {
		JavaRDD<MatrixEntry> simEntriesUpperRdd = simsPerfect.entries().toJavaRDD();
		//JavaRDD<Iterable<MatrixEntry>> groupedSimUpper = simEntriesUpperRdd.groupBy(m->m.i()).values();
		// print similarities
		//groupedSimUpper.foreach(entry->print(entry));

		// create lower triangular
		JavaRDD<MatrixEntry>  simEntriesLowerRdd = simEntriesUpperRdd.map((MatrixEntry entry)-> new MatrixEntry(entry.j(), entry.i(), entry.value()));
		//JavaRDD<Iterable<MatrixEntry>> groupedSimLower = simEntriesLowerRdd.groupBy(m->m.i()).values();
		// print similarities
		//groupedSimLower.foreach(entry->print(entry));

		// combine upper and lower triangular
		JavaRDD<MatrixEntry> simEntriesUnionRdd = simEntriesUpperRdd.union(simEntriesLowerRdd);

		return simEntriesUnionRdd;
	}

	private static  Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>>  getTopk(int k, Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>> list2) {
		
		Integer targetUserId = list2._1;
		
		List<Tuple2<Integer, Integer>> recList = new ArrayList<Tuple2<Integer,Integer>>();
		CollectionUtils.addAll(recList, list2._2.iterator());
		
		if(recList.size() < k){
			k = recList.size();
		}
		
		List<Tuple2<Integer,Integer>> topK = new ArrayList<Tuple2<Integer,Integer>>(recList.subList(0, k));

		
		return new Tuple2(targetUserId, topK);
	}

	private static  Iterable<MatrixEntry> getTopN(int N, Iterable<MatrixEntry> e) {
		List<MatrixEntry> list = new ArrayList<MatrixEntry>();
		CollectionUtils.addAll(list, e.iterator());// TODO what if iterable is too large to fit into memory??
		//Comparator<MatrixEntry> comp = Comparator.comparing(x -> -1* x.value());
		//Collections.sort(list,comp);// TODO if I dont call sort beforehand, I would use this in here--> which one is more effective?

		if(list.size() < N){
			N = list.size();
		}
		List<MatrixEntry> topN = new ArrayList<MatrixEntry>(list.subList(0, N));


		return topN;
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

		int largestUserId = findLargestUserId(invertedIndexMapped);
		//System.out.println(largestUserId);


		// TODO Normally Version 1 and 2 should produce same results, but they do not!!!
		//////////Version 1
		// create itemid-->userid list
		JavaPairRDD<Integer, Iterable<Integer>> invertedIndexGrouped = invertedIndexMapped.groupByKey();
		//invertedIndexGrouped.foreach(t->printTuple2(t));

		JavaRDD<Iterable<Integer>> values = invertedIndexGrouped.values();
		// for each rdd(~entry) find the freq. of users
		retVector = values.map(uList-> countUsers(largestUserId+1, uList));
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

	private static Vector countUsers(Integer size, Iterable<Integer> uList){
		List<Integer> indices = new ArrayList<Integer>();
		List<Double> values = new ArrayList<Double>();
		for (Integer u:uList){
			indices.add(u);
			values.add(1.0);
		}

		// convert List<Integer> to int[]
		int[] indicesArray = indices.stream().mapToInt(i->i).toArray();
		double[] valuesArray = values.stream().mapToDouble(i->i).toArray();

		Vector sv = Vectors.sparse(349, indicesArray, valuesArray);

		return sv;
	}

	private static Vector createVectorOf(int size, Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>> t){
		List<Integer> indices = new ArrayList<Integer>();
		List<Double> values = new ArrayList<Double>();


		Iterable<Tuple2<Integer, Integer>> userFreq = t._2();
		for(Tuple2<Integer, Integer> entry: userFreq){
			Integer index = entry._1;
			Double freq = 1.0; //Double.parseDouble(entry._2);

			indices.add(index);
			values.add(freq);
		}

		// convert List<Integer> to int[] 
		int[] indicesArray = indices.stream().mapToInt(i->i).toArray();
		double[] valuesArray = values.stream().mapToDouble(i->i).toArray();

		Vector sv = Vectors.sparse(size, indicesArray, valuesArray);

		return sv;
	}

	/**
	 * 
	 * @param invertedIndexMapped: itemId-->userId RDD
	 * @return
	 */
	private static int findLargestUserId(
			JavaPairRDD<Integer, Integer> invertedIndexMapped) {
		//invertedIndexMapped.foreach(t->System.out.println(t._1() + " , " + t._2()));
		JavaRDD<Integer> userIds = invertedIndexMapped.map((Tuple2<Integer,Integer> t)->t._2());
		//userIds.foreach(t->System.out.println(t));
		int largestUserId = userIds.reduce((a, b) -> Math.max(a, b));

		return largestUserId;
	}

	private static<T> void printTupleWithIterable(Tuple2<T, Iterable<T>> t){
		System.out.print(t._1() + " , ");

		for(T tVal:t._2){
			System.out.print(tVal + " , ");
		}
		System.out.println();
	}

	private static<T> void printTupleWithTuple(Tuple2<T, Tuple2<T,T>> t){
		System.out.print(t._1() + " , ");
		System.out.print(t._2()._1() + " , ");
		System.out.print(t._2()._2() + " , ");

		System.out.println();
	}

	private static<T> void printIterable(Iterable<T> t){
		for(T tVal:t){
			System.out.print(tVal + " , ");
		}
		System.out.println();
	}

}
