import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import com.google.common.base.Optional;

import scala.Serializable;
import scala.Tuple2;


public class Utils {
	
	public static Vector createVectorOf(int size, Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>> t){
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
	
	public static  Iterable<MatrixEntry> getTopN(int N, Iterable<MatrixEntry> e) {
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
	
	public static Tuple2<Integer, Tuple2<Integer, Integer>> removeOptional(Tuple2<Integer, Tuple2<Integer, Optional<Integer>>> tuple)
	{
		if(tuple._2()._2().isPresent()){
			return new Tuple2<Integer, Tuple2<Integer, Integer>>(tuple._1(), new Tuple2<Integer, Integer>(tuple._2()._1(), tuple._2()._2().get()));
		}else{
			return new Tuple2<Integer, Tuple2<Integer, Integer>>(tuple._1(), new Tuple2<Integer, Integer>(tuple._2()._1(), -1));
		}
	}
		
	public static Vector countVals(Integer size, Iterable<Integer> valList){
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
	
	/**
	 * 
	 * @param invertedIndexMapped: keyId-->valueId RDD
	 * @return
	 */
	public static int findLargestValueId(
			JavaPairRDD<Integer, Integer> invertedIndexMapped) {
		//invertedIndexMapped.foreach(t->System.out.println(t._1() + " , " + t._2()));
		JavaRDD<Integer> userIds = invertedIndexMapped.map((Tuple2<Integer,Integer> t)->t._2());
		//userIds.foreach(t->System.out.println(t));
		int largestUserId = userIds.reduce((a, b) -> Math.max(a, b));

		return largestUserId;
	}
		
	public static JavaRDD<MatrixEntry> calculateCosSim(JavaRDD<Vector> inputVectorRdd){
		RowMatrix matrix = new RowMatrix(inputVectorRdd.rdd());
		// print
		//JavaRDD<Vector> rows = matrix.rows().toJavaRDD();
		//rows.foreach(v->System.out.println(v.toString()));
		
		// NOTE result is listed for upper triangle, i.e. for j indices larger than i indices
		// -- i.e. An n x n sparse upper-triangular matrix of cosine similarities between columns of this matrix.
		CoordinateMatrix simsPerfect = matrix.columnSimilarities();//TODO write your own cosine sim to learn

		// create full similarity matrix (from upper triangular)
		JavaRDD<MatrixEntry> simEntriesUnionRdd = createFullMatrix(simsPerfect);
		//JavaRDD<Iterable<MatrixEntry>> groupedSimUnion = simEntriesUnionRdd.groupBy(m->m.i()).values();
		// print similarities
		//simEntriesUnionRdd.filter(m->m.i()==85).groupBy(m->m.i()).values().foreach(entry->printIterable(entry));
		
		return simEntriesUnionRdd;
	}
	
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
	
	public static JavaPairRDD<Integer,Integer> getTopK(int k, JavaPairRDD<Integer, Integer> recList){
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> countOfRec = recList.mapToPair(tuple->new Tuple2<Tuple2<Integer, Integer>, Integer>(tuple,1));
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> freqOfRec = countOfRec.reduceByKey((x,y)-> x+y);
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> freqOfRecPerUser = freqOfRec.mapToPair(f -> new Tuple2<Integer,Tuple2<Integer, Integer> >(f._1()._1(), new Tuple2<Integer,Integer>(f._1()._2(), f._2())));

		// select k many recItems based on frequencies
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> freqOfRecPerUserSwapped = freqOfRecPerUser.mapToPair(tuple->tuple.swap());	
		JavaPairRDD<Tuple2<Integer, Integer>, Integer>  sorted = freqOfRecPerUserSwapped.sortByKey(new TupleComparator(), false);
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> freqOfRecPerUserReSwapped = sorted.mapToPair(tuple->tuple.swap());
		JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> groupedSortedRec = freqOfRecPerUserReSwapped.groupByKey();
		JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> topk = groupedSortedRec.mapToPair(list->getTopk(k, list));
		
		// return new list of rec. items
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> topKFlattened = topk.flatMapValues(e->e);
		JavaPairRDD<Integer,Integer> topKRecItems = topKFlattened.mapToPair(e->new Tuple2<Integer, Integer>(e._1(),e._2()._1()));
		
		// print
		//topKRecItems.foreach(e->System.out.println(e._1 + " , " + e._2));
		
		return topKRecItems;
	}
	
	private static  Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>>  getTopk(int k, Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>> list2) {
		
		Integer targetUserId = list2._1;
		
		List<Tuple2<Integer, Integer>> recList = new ArrayList<Tuple2<Integer,Integer>>();
		CollectionUtils.addAll(recList, list2._2.iterator());
		
		if(recList.size() < k){
			k = recList.size();
		}
		
		List<Tuple2<Integer,Integer>> topK = new ArrayList<Tuple2<Integer,Integer>>(recList.subList(0, k));

		
		return new Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>>(targetUserId, topK);
	}
	
	/// unused methods
	
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
}

class TupleComparator implements Comparator<Tuple2<Integer, Integer>>, Serializable {
	private static final long serialVersionUID = 8204911924272948547L;

	@Override
	public int compare(Tuple2<Integer, Integer> tuple1, Tuple2<Integer, Integer> tuple2) {
		return tuple1._2 <= tuple2._2 ? 0 : 1;
	}
}