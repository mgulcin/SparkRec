import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.JavaPairRDD;

import scala.Serializable;
import scala.Tuple2;


public class Utils {

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
}

class TupleComparator implements Comparator<Tuple2<Integer, Integer>>, Serializable {
	private static final long serialVersionUID = 8204911924272948547L;

	@Override
	public int compare(Tuple2<Integer, Integer> tuple1, Tuple2<Integer, Integer> tuple2) {
		return tuple1._2 <= tuple2._2 ? 0 : 1;
	}
}