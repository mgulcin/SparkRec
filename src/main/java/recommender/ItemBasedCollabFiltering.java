package recommender;

import main.Main;
import main.Printer;
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
		/*// print neighbors
		Printer.printToFile(Main.logPath,"Neighbors");
		neighbors.foreach(entry->Printer.printToFile(Main.logPath,entry.toString()));
		 */
		// find topk
		JavaPairRDD<Integer,Integer> topKRecItems = RecommenderUtil.selectItemsFromItems(dataFlattened, neighbors, k);
		// print
		Printer.printToFile(Main.logPath,"TopK RecItems");
		topKRecItems.foreach(e->Printer.printToFile(Main.logPath,e._1 + " , " + e._2));

		return topKRecItems;
	}

	/**
	 * 
	 * @param targetUserId: Target user to be given recommendation
	 * @param inputData: Format userid-->itemid. Neighbors past preferences OR all data of all users -- I perform filtering
	 * @param neighbors: Format item-->neighborItemId. List of neighbor items for the target users items OR all items -- I perform filtering
	 * @param k: output list size
	 * @return JavaPairRDD<Integer,Integer> topKRec: Format userid-->itemid. Recommended items for the target user
	 */
	public JavaPairRDD<Integer,Integer> recommend(Integer targetUserId, 
			JavaPairRDD<Integer, Integer> inputData, 
			JavaPairRDD<Integer, Integer> neighborItems, int k){

		// collect the neighbor items of the target user
		JavaRDD<Integer> targetsItemList = inputData.filter(e->e._1.equals(targetUserId)).map(e->e._2);
		HashSet<Integer> targetsItemsSet = new HashSet<Integer>(targetsItemList.collect());//TODO collecting the items here:( can be too big to fit into memory
		JavaPairRDD<Integer, Integer> targetsNeighborItems = neighborItems.filter(tuple->targetsItemsSet.contains(tuple._1));
		/*//print
		Printer.printToFile(Main.logPath,"targetsNeighborItems");
		targetsNeighborItems.foreach(e->Printer.printToFile(Main.logPath, e.toString()));*/

		// find topk by using onlyNeighborsData
		JavaRDD<Integer> topKRecItems = RecommenderUtil.selectItemsFromItems(targetsNeighborItems, k);
		//topKRecItems.foreach(e->Printer.printToFile(Main.logPath, e.toString()));


		// create tuple of target userId-->recommended itemId
		JavaPairRDD<Integer,Integer> topKRec = topKRecItems.mapToPair(item->new Tuple2<Integer,Integer>(targetUserId, item));
		// print
		topKRec.foreach(e->Printer.printToFile(Main.logPath, e._1 + " , " + e._2));

		return topKRec;

	}

	/**
	 * 1- Calculate similarity among items
	 * 2- Collect N-similar items to the ones that are used by the target user
	 * @param dataFlattened: userid-->itemid
	 * @return N similar items to the items: itemid-->similar itemid
	 */
	//TODO Test this
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
		/*// print top-k
		Printer.printToFile(Main.logPath,"TopN items for items");
		topN.foreach(entry->Printer.printToFile(Main.logPath,entry.toString()));*/

		// Select most similar items (i.e. neighbors): itemid-->similar itemid
		JavaPairRDD<Integer,Integer> itemByItemNeighbors = topN.mapToPair((MatrixEntry topElement)->new Tuple2<Integer,Integer>((int)topElement.i(), (int)topElement.j()));

		return itemByItemNeighbors;
	}

	/**
	 * 1- Calculate similarity among items
	 * 2- Collect N-similar items to the ones that are used by the target user
	 * @param targetUserId target's id
	 * @param dataFlattened: userid-->itemid
	 * @return N similar items: userid-->itemid
	 */
	public JavaPairRDD<Integer, Integer> selectNeighbors(Integer targetUserId,
			JavaPairRDD<Integer, Integer> dataFlattened) {

		// calculate cosine similarity of items or re-use already calculated values
		if(simEntries == null){
			calculateSimilarityAmongItems(dataFlattened);
		}

		// filter input data to have info of only target user's items
		JavaPairRDD<Integer, Integer> dataFlattenedOnlyTarget = dataFlattened.filter(e->e._1.equals(targetUserId));
		JavaRDD<Integer> targetsItemList = dataFlattenedOnlyTarget.map(e->e._2);
		HashSet<Integer> targetsItemsSet = new HashSet<Integer>(targetsItemList.collect());//TODO collecting the items here:( can be too big to fit into memory


		JavaRDD<MatrixEntry> simEntriesForTargetsItems = simEntries.filter(matrixEntry->targetsItemsSet.contains((int)matrixEntry.i()) == true);
		/*// print
		Printer.printToFile(Main.logPath,"simEntriesForTargetsItems");
		simEntriesForTargetsItems.foreach(entry->Printer.printToFile(Main.logPath,entry.toString()));*/


		// Select most similar N entries(items) for each item
		JavaRDD<Iterable<MatrixEntry>> groupedSortedSimUnion = simEntriesForTargetsItems.groupBy(m->m.i()).values();
		JavaRDD<MatrixEntry> topN = groupedSortedSimUnion.flatMap((Iterable<MatrixEntry> eList)->Utils.getTopN(N, eList));
		/*// print top-k
		Printer.printToFile(Main.logPath,"TopN items for items");
		topN.foreach(entry->Printer.printToFile(Main.logPath,entry.toString()));*/

		// Select most similar items (i.e. neighbors): itemid-->similar itemid
		JavaPairRDD<Integer,Integer> itemByItemNeighbors = topN.mapToPair((MatrixEntry topElement)->new Tuple2<Integer,Integer>((int)topElement.i(), (int)topElement.j()));

		return itemByItemNeighbors;
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
			//vectorOfUsers.rdd().toJavaRDD().foreach(v->Printer.prin(Main.logPath,v.toString()));
			simEntries = Utils.calculateCosSim(vectorOfItems);
		}
	}



}
