
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.SparkConf;
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

class TupleComparator2 implements Comparator<Tuple2<Integer, Integer>>, Serializable {
	@Override
	public int compare(Tuple2<Integer, Integer> tuple1, Tuple2<Integer, Integer> tuple2) {
		return tuple1._2 <= tuple2._2 ? 0 : 1;
	}
}

public class ItemBasedCollabFiltering implements Serializable {

	public static void performCollaborativeFiltering(String file){
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// load data : userid, itemid1,itemid2,...
		JavaRDD<String> data = sc.textFile(file);

		// parse data: userid, <list of itemid> e.g. u3--><i21,i45,i89>
		JavaRDD<ArrayList<Integer>> dataSplitted = data.map((String line)->splitLine(line));
		JavaPairRDD<Integer,Iterable<Integer>> dataMapped = dataSplitted.mapToPair((ArrayList<Integer> userItemList)->new Tuple2<Integer, Iterable<Integer>>(userItemList.get(0), 
				new ArrayList<Integer>(userItemList.subList(1,userItemList.size()))));
		// print 
		//dataMapped.foreach(s->System.out.println(s));
		JavaPairRDD<Integer, Iterable<Integer>> dataMappedFiltered = dataMapped.filter(dm->(((Collection<Integer>) dm._2()).size() > 0));
		// print 
		//dataMappedFiltered.foreach(s->System.out.println(s));

		// flatten dataMapped: userid, itemid e.g. u3-->i21,u3-->i45, u3-->i89
		JavaPairRDD<Integer, Integer> dataFlattened = dataMappedFiltered.flatMapValues(e->e);
		dataFlattened.cache();// TODO what if I cache more than available resources?? Does it apply kind of LRU??
		// print
		//dataFlattened.foreach(s->System.out.println(s));


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
		int N = 10;
		JavaRDD<MatrixEntry> topN = groupedSortedSimUnion.flatMap((Iterable<MatrixEntry> eList)->getTopN(N, eList));
		// print top-k
		//topK.foreach(entry->System.out.println(entry.toString()));

		// Select most similar items (i.e. neighbors)
		JavaPairRDD<Integer,Integer> neighbors = topN.mapToPair((MatrixEntry topElement)->new Tuple2<Integer,Integer>((int)topElement.i(), (int)topElement.j()));
		neighbors.cache();
		// print neighbors
		//neighbors.foreach(entry->System.out.println(entry.toString()));

		// suggest similar items (neighbior items) to the target users
		// apply join on dataFlattened and neighbors: i.e (targetUser,item) (item, itemNeighbor)--> item, (targetUser, itemNeighbor)

		JavaPairRDD<Integer,Integer> dataFlattenedSwapped = dataFlattened.mapToPair((Tuple2<Integer,Integer>n)->n.swap());
		JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> joined = dataFlattenedSwapped.leftOuterJoin(neighbors);
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedMapped = joined.mapToPair(tuple-> removeOptional(tuple));
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedMappedFiltered = joinedMapped.filter(tuple-> tuple._2()._2() >= 0);

		// get the items that are suggested to target : userid--> recitemId
		JavaPairRDD<Integer,Integer> recList = joinedMappedFiltered.mapToPair(f->f._2());
		//JavaPairRDD<Integer, Iterable<Integer>> recListConcat = recList.groupByKey();
		// print
		//recListConcat.foreach(tuple->printTuple2(tuple));

		// find frequency of recItems per user
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> countOfRec = recList.mapToPair(tuple->new Tuple2<Tuple2<Integer, Integer>, Integer>(tuple,1));
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> freqOfRec = countOfRec.reduceByKey((x,y)-> x+y);
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> freqOfRecPerUser = freqOfRec.mapToPair(f -> new Tuple2(f._1()._1(), new Tuple2(f._1()._2(), f._2())));

		// select k many recItems based on frequncies
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> freqOfRecPerUserSwapped = freqOfRecPerUser.mapToPair(tuple->tuple.swap());	
		JavaPairRDD<Tuple2<Integer, Integer>, Integer>  sorted = freqOfRecPerUserSwapped.sortByKey(new TupleComparator2(), false);
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> freqOfRecPerUserReSwapped = sorted.mapToPair(tuple->tuple.swap());
		// print 
		//freqOfRecPerUserReSwapped.foreach(entry->printTupleWithTuple(entry));
		JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> groupedSortedRec = freqOfRecPerUserReSwapped.groupByKey();

		int k = 3;
		JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> topk = groupedSortedRec.mapToPair(list->getTopk(k, list));
		/*// print top-k
		topk.foreach(t->{
			System.out.print(t._1() + ", ");
			for(Tuple2<Integer, Integer> t2:t._2()){
				System.out.print("(");
				System.out.print(t2._1() + ", ");
				System.out.print(t2._2() );
				System.out.print(")");
			}
			System.out.println();
		});*/

		JavaPairRDD<Integer, Tuple2<Integer, Integer>> topKFlattened = topk.flatMapValues(e->e);
		JavaPairRDD<Integer,Integer> topKRecItems = topKFlattened.mapToPair(e->new Tuple2<Integer, Integer>(e._1(),e._2()._1()));
		// print
		topKRecItems.foreach(e->System.out.println(e._1 + " , " + e._2));

		sc.close();
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





	private static ArrayList<Integer> splitLine(String line) {
		String[] splitted = line.split(",");
		ArrayList<Integer> intVals = new ArrayList<Integer>(splitted.length);
		for(String s: splitted){
			intVals.add(Integer.valueOf(s));
		}

		return intVals;// shoukd I close sc here? why?
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
	 * @param data: userId --> itemIdRDD
	 * @return SparseVector of user freq. for each item
	 */
	private static  JavaRDD<Vector> createVectorOf(JavaPairRDD<Integer, Integer> dataFlattened) {
		JavaRDD<Vector> retVector = null;

		int largestValId = findLargestValId(dataFlattened);
		//System.out.println(largestValId);

		// create userid-->itemid list
		JavaPairRDD<Integer, Iterable<Integer>> indexGrouped = dataFlattened.groupByKey();
		//indexGrouped.foreach(t->printTuple2(t));

		JavaRDD<Iterable<Integer>> values = indexGrouped.values();
		// for each rdd(~entry) find the freq. of items
		retVector = values.map(valList-> countVals(largestValId+1, valList));


		return retVector;
	}

	private static Vector countVals(Integer size, Iterable<Integer> valList){
		List<Integer> indices = new ArrayList<Integer>();
		List<Double> values = new ArrayList<Double>();
		for (Integer value:valList){
			indices.add(value);
			values.add(1.0);
		}

		// convert List<Integer> to int[]
		int[] indicesArray = indices.stream().mapToInt(i->i).toArray();
		double[] valuesArray = values.stream().mapToDouble(i->i).toArray();

		Vector sv = Vectors.sparse(size, indicesArray, valuesArray);

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
	 * @param invertedIndexMapped: keyId-->valId RDD
	 * @return
	 */
	private static int findLargestValId(
			JavaPairRDD<Integer, Integer> indexMapped) {
		//invertedIndexMapped.foreach(t->System.out.println(t._1() + " , " + t._2()));
		JavaRDD<Integer> valIds = indexMapped.map((Tuple2<Integer,Integer> t)->t._2());
		//userIds.foreach(t->System.out.println(t));
		int largestValId = valIds.reduce((a, b) -> Math.max(a, b));

		return largestValId;
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
