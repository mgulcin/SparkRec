import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import scala.Tuple2;

import com.google.common.base.Optional;


public class UserBasedCollabFiltering implements Serializable {

	public void performCollaborativeFiltering(String file){
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// load data : userid, itemid1,itemid2,...
		JavaRDD<String> data = sc.textFile(file);

		// parse data: userid, <list of itemid> e.g. u3--><i21,i45,i89>
		JavaPairRDD<String,Iterable<String>> dataMapped = data.map((String line)->Arrays.asList(line.split(",")))
				.mapToPair((List<String> userItemList)->new Tuple2<String, Iterable<String>>(userItemList.get(0), userItemList.subList(1,userItemList.size())));
		JavaPairRDD<String, Iterable<String>> dataMappedFiltered = dataMapped.filter(dm->(((Collection<String>) dm._2()).size() > 0));
		dataMappedFiltered.cache();// TODO what if I cache more than available resources?? Does it apply kind of LRU??
		// print 
		dataMappedFiltered.foreach(s->System.out.println(s));

		// create inverted index representation: itemId to userId list e.g. i32-->u1
		JavaPairRDD<String, String> invertedIndexMapped = dataMappedFiltered.flatMapToPair(tuple -> createInvertedIndex(tuple));
		//JavaPairRDD<String, Iterable<String>> invertedIndexGrouped = invertedIndexMapped.groupByKey();
		// print inverted list
		//invertedIndexMapped.foreach(t->System.out.println(t._1() + " , " + t._2()));
		//invertedIndexGrouped.foreach(t->printTuple2(t));

		// calculate cosine similarity of users 
		JavaRDD<Vector> vectorOfUsers = createVectorOf(invertedIndexMapped);
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

		// Select most similar k entries 
		int k = 2;
		JavaRDD<MatrixEntry> topK = groupedSortedSimUnion.flatMap((Iterable<MatrixEntry> eList)->getTopK(k, eList));
		// print top-k
		//topK.foreach(entry->System.out.println(entry.toString()));

		// Select most similar users (i.e. neighbors)
		JavaPairRDD<String,String> neighbors = topK.mapToPair((MatrixEntry topElement)->new Tuple2(String.valueOf(topElement.i()), String.valueOf(topElement.j())));
		neighbors.cache();
		// print neighbors
		neighbors.foreach(entry->System.out.println(entry.toString()));

		/*// apply join on dataMapped and neighbors: i.e (targetUser,neighbor) (neighbor,<itemList>) --> neighbor, (targetUser, <itemList>)
		JavaPairRDD<String,String> neighborsSwapped = neighbors.mapToPair((Tuple2<String,String>n)->n.swap());
		//neighborsSwapped.foreach(entry->System.out.println(entry.toString()));
		//System.out.println(neighborsSwapped.count());
		//System.out.println(dataMapped.count());

		Broadcast<Map<String, String>> neighborsSwappedMap = sc.broadcast(neighborsSwapped.collectAsMap());
				JavaPairRDD<String, Iterable<String>> filtered = dataMapped.filter(dm->(neighborsSwappedMap.value().get(dm._1) != null));
		 


		JavaPairRDD<String, Tuple2<Iterable<String>, Optional<String>>> joined = dataMappedFiltered.leftOuterJoin(neighborsSwapped);
		JavaPairRDD<String, Tuple2<Iterable<String>, String>> joinedMapped = joined.mapToPair(tuple-> removeOptional(tuple));


		// get the items that are suggested to target :
		//JavaPairRDD<String,Iterable<String>> recList = joinedMapped.mapToPair(f->f._2());
		//JavaPairRDD<String, Iterable<String>> recListConcat = recList.reduceByKey((x,y)-> Iterables.concat(x,y));


		//System.out.println(joinedMapped.count());
		joinedMapped.collect();
		//foreach(e->deneme(e));
*/	}
	




	private boolean isElementOf(Tuple2<String, Iterable<String>> dm,
			JavaPairRDD<String, String> neighborsSwapped) {
		boolean retVal = false;
		JavaRDD<Boolean> temp = neighborsSwapped.map(n-> n._1.equals(dm._1())).filter(r->r==true);
		long trueCount = temp.count();
		if(trueCount > 0){
			retVal = true;
		}
		return retVal;
	}

	private Tuple2<String, Tuple2<Iterable<String>,String>> removeOptional(Tuple2<String, Tuple2<Iterable<String>, Optional<String>>> tuple)
	{
		if(tuple._2()._2().isPresent()){
			return new Tuple2<String, Tuple2<Iterable<String>, String>>(tuple._1(), new Tuple2(tuple._2()._1(), tuple._2()._2().get()));
		}else{
			return new Tuple2<String, Tuple2<Iterable<String>, String>>(tuple._1(), new Tuple2(tuple._2()._1(), "dummy"));
		}
	};


	private  JavaRDD<MatrixEntry> createFullMatrix(
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



	private  Iterable<MatrixEntry> getTopK(int k, Iterable<MatrixEntry> e) {
		List<MatrixEntry> list = new ArrayList<MatrixEntry>();
		CollectionUtils.addAll(list, e.iterator());// TODO what if iterable is too large to fit into memory??
		//Comparator<MatrixEntry> comp = Comparator.comparing(x -> -1* x.value());
		//Collections.sort(list,comp);// TODO if I dont call sort beforehand, I would use this in here--> which one is more effective?

		if(list.size() < k){
			k = list.size();
		}
		List<MatrixEntry> topK = list.subList(0, k);


		return topK;
	}



	/**
	 * @param data: itemId-->userId RDD
	 * @return SparseVector of user freq. for each item
	 */
	private  JavaRDD<Vector> createVectorOf(
			JavaPairRDD<String, String> invertedIndexMapped) {
		JavaRDD<Vector> retVector = null;
		/*// count total number of users
		long userCount = countUsers(invertedIndexMapped);
		System.out.println(userCount);*/
		int largestUserId = findLargestUserId(invertedIndexMapped);
		//System.out.println(largestUserId);


		// TODO Normally Version 1 and 2 should produce same results, but they do not!!!
		//////////Version 1
		// create itemid-->userid list
		JavaPairRDD<String, Iterable<String>> invertedIndexGrouped = invertedIndexMapped.groupByKey();
		//invertedIndexGrouped.foreach(t->printTuple2(t));

		JavaRDD<Iterable<String>> values = invertedIndexGrouped.values();
		// for each rdd(~entry) find the freq. of users
		retVector = values.map(uList-> countUsers(largestUserId+1, uList));
		//////////Version 1
		////////// Version 2
		/*// count freq. of a user for each item : (itemId, userId)-->freq
		JavaPairRDD<Tuple2<String,String>, Integer> pairs = invertedIndexMapped.mapToPair((Tuple2<String,String> t)-> new Tuple2<Tuple2<String,String>,Integer>(t,1));
		JavaPairRDD<Tuple2<String,String>, Integer> counts = pairs.reduceByKey((x,y)-> x+y); //  pairs.reduceByKey(Integer::sum);
		//counts.foreach(t->System.out.println(t._1() + " , " + t._2()));

		// create itemid-->(userid,freq) and group by itemId
		JavaPairRDD<String, Tuple2<String, Integer>> userFreqPerItem = counts.mapToPair((Tuple2<Tuple2<String,String>, Integer> t)-> new Tuple2<String,Tuple2<String,Integer>>(t._1()._1(), new Tuple2<String,Integer>(t._1()._2(),t._2())));		
		JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> userFreqListPerItem = userFreqPerItem.groupByKey();
		//userFreqListPerItem.foreach(t->printTuple2(t));
		retVector = userFreqListPerItem.map((Tuple2<String, Iterable<Tuple2<String, Integer>>> t)-> createVectorOf((largestUserId+1), t));
		 */
		//////////Version 2

		return retVector;
	}

	private Vector countUsers(Integer size, Iterable<String> uList){
		List<Integer> indices = new ArrayList<Integer>();
		List<Double> values = new ArrayList<Double>();
		for (String u:uList){
			indices.add(Integer.parseInt(u));
			values.add(1.0);
		}

		// convert List<Integer> to int[]
		int[] indicesArray = indices.stream().mapToInt(i->i).toArray();
		double[] valuesArray = values.stream().mapToDouble(i->i).toArray();

		Vector sv = Vectors.sparse(size, indicesArray, valuesArray);

		return sv;
	}

	private Vector createVectorOf(int size, Tuple2<String, Iterable<Tuple2<String, Integer>>> t){
		List<Integer> indices = new ArrayList<Integer>();
		List<Double> values = new ArrayList<Double>();


		Iterable<Tuple2<String, Integer>> userFreq = t._2();
		for(Tuple2<String, Integer> entry: userFreq){
			Integer index = Integer.parseInt(entry._1);
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
	private int findLargestUserId(
			JavaPairRDD<String, String> invertedIndexMapped) {
		//invertedIndexMapped.foreach(t->System.out.println(t._1() + " , " + t._2()));
		JavaRDD<Integer> userIds = invertedIndexMapped.map((Tuple2<String,String> t)-> Integer.valueOf(t._2()));
		//userIds.foreach(t->System.out.println(t));
		int largestUserId = userIds.reduce((a, b) -> Math.max(a, b));

		return largestUserId;
	}

	private long countUsers(
			JavaPairRDD<String, String> invertedIndexMapped) {
		JavaPairRDD<String, Integer> pairs = invertedIndexMapped.mapToPair((Tuple2<String,String> t)-> new Tuple2<String,Integer>(t._2(),1));
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey((x,y)-> x+y); //  pairs.reduceByKey(Integer::sum);
		//counts.foreach(t->System.out.println(t._1() + " , " + t._2()));
		long userCount = counts.count();

		return userCount;
	}

	private static<T> void printTuple2(Tuple2<T, Iterable<T>> t){
		System.out.print(t._1() + " , ");

		for(T tVal:t._2){
			System.out.print(tVal + " , ");
		}
		System.out.println();
	}

	private static<T> void print(Iterable<T> t){
		for(T tVal:t){
			System.out.print(tVal + " , ");
		}
		System.out.println();
	}


	/**
	 * 
	 * @param inputTuple: userId,<list of itemIds>
	 * @return inverted map from itemId->userId
	 */
	private List<Tuple2<String,String>> createInvertedIndex(Tuple2<String,Iterable<String>> inputTuple){
		List<Tuple2<String,String>> returnList = new ArrayList<Tuple2<String,String>>();

		String uId= inputTuple._1();
		for (String iId:inputTuple._2()){
			Tuple2<String,String> newTuple = new Tuple2<String, String>(iId, uId);
			returnList.add(newTuple);
		}
		return returnList;
	}

	/////////////////////////// UNUSED METHODS ////////////////////////////////
	/**
	 * Same as printTuple2, but written with inline function
	 * @param data
	 */
	@SuppressWarnings({ "unused", "serial" })
	private void print(
			JavaPairRDD<String, Iterable<String>> data) {
		data.foreach(new VoidFunction<Tuple2<String,Iterable<String>>>() {

			@Override
			public void call(Tuple2<String, Iterable<String>> t) throws Exception {
				System.out.print(t._1() + " , ");

				for(String tVal:t._2){
					System.out.print(tVal + " , ");
				}
				System.out.println();
			}
		});


	}

	/**
	 * Same as createInvertedIndex and its call, but written with inline function
	 * @param data
	 * @return
	 */
	public JavaPairRDD<String, String> invertedIndexMap(JavaRDD<String> data) {		
		// data contains lines of userId,<comma separated list of itemIds>

		@SuppressWarnings("serial")
		JavaPairRDD<String, String> inverted = data.flatMapToPair(
				new PairFlatMapFunction<String, String, String>() {
					public Iterable<scala.Tuple2<String,String>> call(String inputLine){
						List<Tuple2<String,String>> returnList = new ArrayList<Tuple2<String,String>>();

						String[] sarray = inputLine.split(",");

						String uId= sarray[0];
						for (int i = 1; i < sarray.length; i++){
							String gId= sarray[i];

							Tuple2<String,String> newTuple = new Tuple2<String, String>(gId, uId);
							returnList.add(newTuple);
						}


						return returnList;
					}
				}

				);

		return inverted;

	}


	/**
	 * TODO read this too: https://gist.github.com/vrilleup/9e0613175fab101ac7cd
	 * https://spark.apache.org/docs/latest/mllib-clustering.html
	 * @return sparse vector
	 */
	public JavaRDD<IndexedRow> loadAndParseData(JavaSparkContext sc,
			JavaRDD<String> data){
		// Parse data
		@SuppressWarnings("serial")
		JavaRDD<IndexedRow> parsedData = data.map(
				new Function<String, IndexedRow>() {

					public IndexedRow call(String s) {
						String[] sarray = s.split(",");
						int[] indices = new int[sarray.length-1];
						double[] values = new double[sarray.length-1];
						for (int i = 1; i < sarray.length; i++){
							indices[i-1] = Integer.parseInt(sarray[i]);
							values[i-1] = 1.0;
						}
						Vector sv = Vectors.sparse(7736, indices, values);//TODO hard coded vector sze!!
						int index =  Integer.parseInt(sarray[0]);
						IndexedRow irow = new IndexedRow(index, sv);
						return irow;
					}
				}
				);
		parsedData.cache();

		return parsedData;
	}



}
