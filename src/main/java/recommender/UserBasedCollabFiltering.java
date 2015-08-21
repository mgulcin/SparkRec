package recommender;

import java.util.HashSet;
import java.util.List;

import main.Printer;
import main.Utils;
import main.Main;

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

	// number of neighbors
	private int N;

	public UserBasedCollabFiltering(int n) {
		N = n;
	}


	public int getN() {
		return N;
	}


	public void setN(int n) {
		N = n;
	}

	/**
	 * 1- Calculate similarity among users
	 * 2- Collect N-similar users (neighbors) to the target user
	 * 3- Collect k-many items by integrating the items used by the neighbors
	 * 4- Return top-k items
	 * @param sc
	 * @param dataFlattened: userid-->itemid
	 * @param k: output list size
	 * @return recommended items
	 */
	public JavaPairRDD<Integer,Integer> performBatchRecommendation(JavaSparkContext sc, 
			JavaPairRDD<Integer, Integer> dataFlattened, int k){

		// Select most similar users (i.e. neighbors)
		JavaPairRDD<Integer,Integer> neighbors = selectNeighbors(dataFlattened);
		neighbors.cache();
		/*// print neighbors
		Printer.printToFile(Main.logPath, "Neighbors: ");
		neighbors.foreach(entry->Printer.printToFile(Main.logPath, entry._1 + ", " + entry._2  ));*/

		// find topk
		JavaPairRDD<Integer,Integer> topKRecItems = RecommenderUtil.selectItemsFromNeighbors(dataFlattened, neighbors, k);
		/*// print
		Printer.printToFile(Main.logPath, "TopK: ");
		topKRecItems.foreach(e->Printer.printToFile(Main.logPath, e._1 + " , " + e._2));*/

		return topKRecItems;
	}
	
	/**
	 * 
	 * @param targetUserId: Target user to be given recommendation
	 * @param inputData: Format userid-->itemid. Neighbors past preferences OR all data of all users -- I perform filtering
	 * @param neighbors: Format targetId-->neighborId. List of users for the target user OR all users -- I perform filtering
	 * @param k: output list size
	 * @return JavaPairRDD<Integer,Integer> topKRec: Format userid-->itemid. Recommended items for the target user
	 */
	public JavaPairRDD<Integer,Integer> recommend(Integer targetUserId, 
			JavaPairRDD<Integer, Integer> inputData, JavaPairRDD<Integer, Integer> neighbors, int k){
		
		// collect the neighbors of the target user
		JavaRDD<Integer> targetsNeighbors = neighbors.filter(tuple->tuple._1.equals(targetUserId)).map(tuple->tuple._2);
		//targetsNeighbors.foreach(e->Printer.printToFile(Main.logPath, e.toString()));
		
		// filter input data to have info of only neighbors
		HashSet<Integer> targetsNeighborsSet = new HashSet<Integer>(targetsNeighbors.collect());//TODO collecting the neighbors here:( Ok only size N
		JavaPairRDD<Integer, Integer> onlyNeighborsData = inputData.filter(tuple->targetsNeighborsSet.contains(tuple._1) == true);
		//onlyNeighborsData.foreach(e->Printer.printToFile(Main.logPath, e._1 + " , " + e._2));
		
		
		// find topk by using onlyNeighborsData
		JavaRDD<Integer> topKRecItems = RecommenderUtil.selectItemsFromNeighbors(onlyNeighborsData, k);
		//topKRecItems.foreach(e->Printer.printToFile(Main.logPath, e.toString()));
		
	
		// create tuple of target userId-->recommended itemId
		JavaPairRDD<Integer,Integer> topKRec = topKRecItems.mapToPair(item->new Tuple2<Integer,Integer>(targetUserId, item));
		// print
		//topKRec.foreach(e->Printer.printToFile(Main.logPath, e._1 + " , " + e._2));

		return topKRec;
	}

	//TODO Gives error. I guess I cannot use an Rdd inside a method
	private boolean doesContain(JavaRDD<Integer> targetsNeighbors, Integer neighborId) {
		boolean retVal = false;

		// create a dummy JavaPairRDD
		JavaPairRDD<Integer, Boolean> targetsNeighborsDummyMap = targetsNeighbors.mapToPair(e->new Tuple2<Integer, Boolean>(e,null));
		//targetsNeighborsDummyMap.foreach(e->Printer.printToFile(Main.logPath, e._1 + " , " + e._2));
		
		// search the dummy JavaPairRDD if it contains neighborId as key or not
		List<Boolean> retList = targetsNeighborsDummyMap.lookup(neighborId);
		//Printer.printToFile(Main.logPath,retList.toString());
		
		// if at least one dummy JavaPairRDD exist, then neighborId is in targetsNeighbors
		if(retList != null && retList.isEmpty() == false){
			retVal = true;
		}
		
		return retVal;
	}


	/**
	 * 1- Calculate similarity among users based on their past preferences (e.g. movies rated, products bought, venues checked in)
	 * 2- Collect N-similar users (neighbors) to the target user
	 * @param dataFlattened: Input data with the format userid-->itemid
	 * @return most similar N neighbors 
	 */
	public JavaPairRDD<Integer,Integer> selectNeighbors(JavaPairRDD<Integer, Integer> dataFlattened){
		// calculate cosine similarity of users 
		JavaRDD<Vector> vectorOfUsers = RecommenderUtil.createVectorOfNeighbors(dataFlattened);
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
		JavaRDD<MatrixEntry> topN = groupedSortedSimUnion.flatMap((Iterable<MatrixEntry> eList)->Utils.getTopN(N, eList));
		// print top-k
		//topK.foreach(entry->System.out.println(entry.toString()));

		// Select most similar users (i.e. neighbors)
		JavaPairRDD<Integer,Integer> neighbors = topN.mapToPair((MatrixEntry topElement)->new Tuple2<Integer,Integer>((int)topElement.i(), (int)topElement.j()));
		neighbors.cache();
		// print neighbors
		//neighbors.foreach(entry->System.out.println(entry.toString()));
		
		return neighbors;
	}
	
}
