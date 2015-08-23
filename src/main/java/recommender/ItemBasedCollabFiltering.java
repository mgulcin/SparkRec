package recommender;

import main.Main;
import main.Utils;

import java.util.HashSet;
import java.util.List;

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

	// similarity among users
	JavaRDD<MatrixEntry> simEntries;


	public ItemBasedCollabFiltering(int n) {
		super();
		N = n;
		simEntries= null;
	}


	public void setN(int n) {
		N = n;
	}

	/**
	 * 1- Calculate similarity among items
	 * 2- Collect N-similar items to the ones that are used by the target user
	 * 3- Collect k-many items by integrating the items identified in step-2 
	 * 4- Return top-k items
	 * @param dataFlattened: userid-->itemid
	 * @param k: output list size
	 * @return recommended items
	 */
	public JavaPairRDD<Integer,Integer> performBatchRecommendation(JavaPairRDD<Integer, Integer> dataFlattened, int k){

		// Select most similar items (i.e. neighbors)
		// TODO directly neighbor items can be recommended too
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
	 * 
	 * @param targetUserId: Target user to be given recommendation
	 * @param inputData: Format userid-->itemid. Neighbors past preferences OR all data of all users -- I perform filtering
	 * @param neighbors: Format targetId-->neighborItemId. List of neighbor items for the target user OR all items -- I perform filtering
	 * @param k: output list size
	 * @return JavaPairRDD<Integer,Integer> topKRec: Format userid-->itemid. Recommended items for the target user
	 */
	public JavaPairRDD<Integer,Integer> recommend(Integer targetUserId, 
			JavaPairRDD<Integer, Integer> inputData, 
			JavaPairRDD<Integer, Integer> neighborItems, int k){

		// collect the neighbor items of the target user
		JavaRDD<Integer> targetsNeighborItems = neighborItems.filter(tuple->tuple._1.equals(targetUserId)).map(tuple->tuple._2);
		//targetsNeighborItems.foreach(e->Printer.printToFile(Main.logPath, e.toString()));

		// filter input data to have info of only neighbor items
		HashSet<Integer> targetsNeighborsSet = new HashSet<Integer>(targetsNeighborItems.collect());//TODO collecting the neighbors here:( Ok size is only N
		JavaPairRDD<Integer, Integer> onlyNeighborItemsData = inputData.filter(tuple->targetsNeighborsSet.contains(tuple._2) == true);
		//onlyNeighborsData.foreach(e->Printer.printToFile(Main.logPath, e._1 + " , " + e._2));

		// find topk by using onlyNeighborsData
		JavaRDD<Integer> topKRecItems = RecommenderUtil.selectItemsFromItems(onlyNeighborItemsData, k);
		//topKRecItems.foreach(e->Printer.printToFile(Main.logPath, e.toString()));


		// create tuple of target userId-->recommended itemId
		JavaPairRDD<Integer,Integer> topKRec = topKRecItems.mapToPair(item->new Tuple2<Integer,Integer>(targetUserId, item));
		// print
		//topKRec.foreach(e->Printer.printToFile(Main.logPath, e._1 + " , " + e._2));

		return topKRec;

	}

	/**
	 * 1- Calculate similarity among items
	 * 2- Collect N-similar items to the ones that are used by the target user
	 * @param dataFlattened: userid-->itemid
	 * @return N similar items: userid-->itemid
	 */
	private JavaPairRDD<Integer, Integer> selectNeighbors(
			JavaPairRDD<Integer, Integer> dataFlattened) {

		// calculate cosine similarity of items or re-use already calculated values
		if(simEntries == null){
			calculateSimilarityAmongItems(dataFlattened);
		}

		// Create sorted list (based on similarity) of other items	
		// sort by value and group by i // TODO does this always return sorted list after groupby?
		JavaRDD<MatrixEntry> sortedSimEntriesUnionRdd = simEntries.sortBy(x->x.value(),false,1);
		JavaRDD<Iterable<MatrixEntry>> groupedSortedSimUnion = sortedSimEntriesUnionRdd.groupBy(m->m.i()).values();
		//groupedSortedSimUnion.foreach(entry->print(entry));

		// Select most similar N entries(items) for each item
		JavaRDD<MatrixEntry> topN = groupedSortedSimUnion.flatMap((Iterable<MatrixEntry> eList)->Utils.getTopN(N, eList));
		// print top-k
		//topN.filter(m->m.i()==85).foreach(entry->System.out.println(entry.toString()));

		// Select most similar items (i.e. neighbors): base itemid-->similar itemid
		JavaPairRDD<Integer,Integer> itemByItemNeighbors = topN.mapToPair((MatrixEntry topElement)->new Tuple2<Integer,Integer>((int)topElement.i(), (int)topElement.j()));

		// for each target user create list of similar items 
		// i.e. combine (targetuser->item) & (item->similar item) and return targetuser->similar item
		// TODO Control this!!, simEntries contain item-to-item similarity
		
		JavaPairRDD<Integer, Integer> neighbors = null;
		return neighbors;
	}

	/**
	 * 1- Calculate similarity among items
	 * 2- Collect N-similar items to the ones that are used by the target user
	 * @param targetUserId target's id
	 * @param dataFlattened: userid-->itemid
	 * @return N similar items: userid-->itemid
	 */
	private JavaPairRDD<Integer, Integer> selectNeighbors(Integer targetUserId,
			JavaPairRDD<Integer, Integer> dataFlattened) {

		// calculate cosine similarity of items or re-use already calculated values
		if(simEntries == null){
			calculateSimilarityAmongItems(dataFlattened);
		}
		return dataFlattened;

		//TODO finish this code after reviewing selectNeighbors(JavaPairRDD<Integer, Integer> dataFlattened)
	}

	/**
	 * 
	 * @param dataFlattened: Input data with the format userid-->itemid
	 */
	private void calculateSimilarityAmongItems(JavaPairRDD<Integer, Integer> dataFlattened){
		if(simEntries == null){
			// calculate cosine similarity of items
			JavaRDD<Vector> vectorOfItems = RecommenderUtil.createVectorOfItems(dataFlattened);
			// print
			//vectorOfUsers.rdd().toJavaRDD().foreach(v->System.out.println(v.toString()));
			simEntries = Utils.calculateCosSim(vectorOfItems);
		}
	}



}
