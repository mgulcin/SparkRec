package recommender;

import main.Printer;
import main.Utils;

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
	public JavaPairRDD<Integer,Integer> performRecommendation(JavaSparkContext sc, 
			JavaPairRDD<Integer, Integer> dataFlattened, int k){

		// Select most similar users (i.e. neighbors)
		JavaPairRDD<Integer,Integer> neighbors = selectNeighbors(dataFlattened);
		neighbors.cache();
		// print neighbors
		//neighbors.foreach(entry->System.out.println(entry.toString()));

		// find topk
		JavaPairRDD<Integer,Integer> topKRecItems = RecommenderUtil.selectItemsFromNeighbors(dataFlattened, neighbors, k);
		// print
		//topKRecItems.foreach(e->System.out.println(e._1 + " , " + e._2));

		return topKRecItems;
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
