package recommender;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import main.Printer;
import main.Utils;

import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;

import com.google.common.base.Optional;

import scala.Serializable;
import scala.Tuple2;



public class MultiObjectiveRec implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private int N;


	public MultiObjectiveRec(int N) {
		super();
		this. N = N;	
	}


	/**
	 * 1- Return top-k items 
	 * @param sc
	 * @param inputDataList: input data list. Each data is formatted as: userid--> itemId 
	 * @param k: outputList size
	 * @return recommended items, e.g. userid--> itemId 
	 */
	public JavaPairRDD<Integer, Integer> performRecommendation(JavaSparkContext sc,
			List<JavaPairRDD<Integer, Integer>> inputDataList, int k){

		// for each inputDataType(feature): 
		// calculate cosine similarity of users for each inputDataType(feature)
		// and get most similar neighbors 
		List<JavaRDD<MatrixEntry>> simList = new ArrayList<JavaRDD<MatrixEntry>>();
		for(JavaPairRDD<Integer, Integer> dataFlattened:inputDataList){
			// create vector representation
			JavaRDD<Vector> vectorOfUsers = createVectorOf(dataFlattened);
			//vectorOfUsers.foreach(v->System.out.println(v.toString()));

			// calculate cos sim
			JavaRDD<MatrixEntry> simEntriesUnionRdd = Utils.calculateCosSim(vectorOfUsers);
			// print similarities
			//JavaRDD<Iterable<MatrixEntry>> groupedSimUnion = simEntriesUnionRdd.groupBy(m->m.i()).values();
			//groupedSimUnion.foreach(entry->print(entry));

			/*
			 * TODO may be just using the most similar users can improve scalability
			 * //take best N users (based on similarity) 
			// 	+ the ones that have the equal sim value to the last one
			JavaRDD<MatrixEntry> mostSimUsers =  selectMostSimilars(simEntriesUnionRdd);
			mostSimUsers.cache(); // should I cache here or in the output list

			// add to the list
			simList.add(mostSimUsers);
			 */

			// add to the list
			simList.add(simEntriesUnionRdd);
		}

		// select N non-domainated users
		JavaPairRDD<Integer, Integer> neighbors = selectNonDominateds(simList, sc);

		// find items to recommend 
		// TODO copied from UserBased collab filtering--> find a better design!!
		JavaPairRDD<Integer, Integer> baseData = inputDataList.get(0);//TODO currently using the fisrt data type as base, make parametric!!
		JavaPairRDD<Integer,Integer> topKRecItems = findItemsToSuggest(k, neighbors, baseData);

		return topKRecItems ;
	}


	private JavaPairRDD<Integer,Integer>  findItemsToSuggest(int k, JavaPairRDD<Integer, Integer> neighbors,
			JavaPairRDD<Integer, Integer> baseData) {
		// apply join on dataMapped and neighbors: i.e (targetUser,neighbor) (neighbor,<itemList>) --> neighbor, (targetUser, <itemList>)
		JavaPairRDD<Integer,Integer> neighborsSwapped = neighbors.mapToPair((Tuple2<Integer,Integer>n)->n.swap());
		//neighborsSwapped.foreach(entry->System.out.println(entry.toString()));
		//System.out.println(neighborsSwapped.count());
		//System.out.println(dataMapped.count());


		JavaPairRDD<Integer, Tuple2<Integer, Optional<Integer>>> joined = baseData.leftOuterJoin(neighborsSwapped);
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedMapped = joined.mapToPair(tuple-> Utils.removeOptional(tuple));
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedMappedFiltered = joinedMapped.filter(tuple-> tuple._2()._2() >= 0);

		// get the items that are suggested to target : userid--> recitemId
		JavaPairRDD<Integer,Integer> recList = joinedMappedFiltered.mapToPair(f->new Tuple2<Integer,Integer>(f._2()._2(),f._2()._1()));
		//JavaPairRDD<Integer, Iterable<Integer>> recListConcat = recList.groupByKey();
		// print
		//recListConcat.filter(x->x._1==1).foreach(tuple->Printer.printTupleWithIterable(tuple));

		// find topk
		JavaPairRDD<Integer,Integer> topKRecItems = Utils.getTopK(k, recList);
		// print
		//topKRecItems.foreach(e->System.out.println(e._1 + " , " + e._2));

		return topKRecItems;	

	}


	private JavaPairRDD<Integer, Integer> selectNonDominateds(List<JavaRDD<MatrixEntry>> simList, JavaSparkContext sc) {

		// convert list<matrixEntry> to (targetUser,otherUser) -->FeatureSim pairs (for all type of data)
		JavaPairRDD<Tuple2<Long,Long>,FeatureSim> allSimPairs = createSimPairs(simList);

		/*// print for targetUser=76
		allSimPairs.filter(e->e._1._1 == 76).foreach(e->System.out.println("Target: " + e._1._1 +
				" Other: " + e._1._2 +
				" Sim: " + e._2));*/

		// all target users
		JavaRDD<Long> targetUsers = allSimPairs.keys().distinct().map(e->e._1).distinct();
		/*// print
				System.out.println("#targetUsers: " +targetUsers.count());
		 */		

		// all candidate neighbors for each target user : Cartesian product of targets except themselves
		JavaPairRDD<Long,Long> candidateUsers = allSimPairs.mapToPair(entry->entry._1).distinct();
		// print
		System.out.println("#candidateUsers for 76: " + candidateUsers.filter(e->e._1 == 76).count());
		
		

		// initialize neighbors & neighborCounts
		JavaPairRDD<Long, Integer> neighborCounts = targetUsers.mapToPair(e->new Tuple2<Long,Integer>(e,0));
		/*// print
				System.out.println("#neighborCounts: " +neighborCounts.count());
				neighborCounts.foreach(e->System.out.println("#Neighbors: " + e._1 + " " + e._2));*/

		JavaPairRDD<Long, Long> neighbors = targetUsers.mapToPair(e->new Tuple2<Long,Long>(e,null));

		// remove already selected neighbors from candidateUsers - not to re-select them
		candidateUsers = candidateUsers.subtract(neighbors).distinct();
		System.out.println("#candidateUsers for 76: " + candidateUsers.filter(e->e._1 == 76).count());

		// filter out target users who have already collected predefined number of neighbors 
		// actually here it is same as the initial targets
		targetUsers = neighborCounts.filter(e->e._2 < N).keys();

		// select neighbors
		while(targetUsers.count() > 0){
			JavaPairRDD<Long, Long> neighborsTemp = findNeighbors(allSimPairs, targetUsers, candidateUsers);

			//print 
			neighbors.filter(e->e._1 == 76).foreach(e->System.out.println("Neighbors(before) for 76: " + e._1 + " " + e._2));
			neighborsTemp.filter(e->e._1 == 76).foreach(e->System.out.println("Neighbors(temp) for 76: " + e._1 + " " + e._2));

			// combine older neighbors info with newly found neighbors
			neighbors = neighbors.union(neighborsTemp).distinct().filter(e->e._2!=null);

			//print 
			neighbors.filter(e->e._1 == 76).foreach(e->System.out.println("Neighbors(after) for 76: " + e._1 + " " + e._2));

			// update neighborsCount
			neighborCounts = neighbors.mapToPair(e-> new Tuple2<Long,Integer>(e._1,1)).reduceByKey((x,y)->x+y);

			// print
			neighborCounts.filter(e->e._1 == 76).foreach(e->System.out.println("#Neighbors: " + e._1 + " " + e._2));

			// filter out target users who have already collected predefined number of neighbors 
			targetUsers = neighborCounts.filter(e->e._2 < N).keys();
			
			// remove already selected neighbors from candidateUsers - not to re-select them
			candidateUsers = candidateUsers.subtract(neighbors).distinct();
			System.out.println("#candidateUsers for 76: " + candidateUsers.filter(e->e._1 == 76).count());
			
			// remove already selected neighbors from allSimPairs - not to re-select them
		TODO	!! I could not remove vals for the ones that i found as neighbor
			
			Map<Long,Long> neighborsMap = neighbors.collectAsMap();// is it a good idea to collect neighbors in a loop??
			for(Entry<Long, Long> e : neighborsMap.entrySet()){
				System.out.println("Map : " + e.getKey() + " " + e.getValue());
			}
			allSimPairs = allSimPairs.filter(e->(neighborsMap.get(e._1._1)!=e._1._2));
			//neighborsMap.clear();//should i clear it here??

			//print target:76 
			
			allSimPairs.filter(e->e._1._1 == 76).foreach(e->System.out.println("Target: " + e._1._1 +
					" Other: " + e._1._2 +
					" Sim: " + e._2));
			
			

		}

		JavaPairRDD<Integer, Integer> retneighbors = null;
		return retneighbors;
	}


	private JavaPairRDD<Long, Long> findNeighbors(JavaPairRDD<Tuple2<Long, Long>, FeatureSim> allSimPairs, JavaRDD<Long> targetUsers, JavaPairRDD<Long, Long> candidateUsers) {

		// TODO used groupby :( can I combine this and the next with reduceBy??
		JavaPairRDD<Tuple2<Long, Long>, Iterable<FeatureSim>> allPairsGrouped = allSimPairs.groupByKey();

		// targetUser --> (otherUser, FeatureSim list)
		JavaPairRDD<Long,Tuple2<Long,Iterable<FeatureSim>>> allPairs = allPairsGrouped.mapToPair(entry->new Tuple2(entry._1._1, new Tuple2(entry._1._2,entry._2)));

		/*// print for targetUser=76
		allPairs.filter(e->e._1 == 76).foreach(e->Printer.printSimList(e));*/


		// cartesian: (targetUser --> (otherUser, FeatureSim list)) --> (targetUser --> (otherUser, FeatureSim list))
		// get only if targetUser's are same
		// and update entry to targetUser --> ((otherUser, FeatureSim list), (otherUser, FeatureSim list))
		JavaPairRDD<Long, Tuple2<Tuple2<Long,Iterable<FeatureSim>>, Tuple2<Long,Iterable<FeatureSim>>>> cartesianAllPairs = 
				allPairs.cartesian(allPairs).filter(tupleOfTuple->tupleOfTuple._1._1.equals(tupleOfTuple._2._1)).
				mapToPair(tupleOfTuple-> new Tuple2<Long, Tuple2<Tuple2<Long,Iterable<FeatureSim>>, Tuple2<Long,Iterable<FeatureSim>>>>
				(tupleOfTuple._1._1, new Tuple2(tupleOfTuple._1._2,tupleOfTuple._2._2)));

		/*//print target:76 user1:152
		System.out.println(cartesianAllPairs.count());
		cartesianAllPairs.filter(e->e._1 == 76 && e._2._1._1==152).foreach(x->Printer.printCartesianSimList(x));*/

		// for each target user, find if user1 dominates user2
		JavaPairRDD<Long, Tuple2<Tuple2<Long,Long>, Boolean>> dominanceInfo  = cartesianAllPairs.mapToPair(tupleOfCartesian->
		new Tuple2<Long, Tuple2<Tuple2<Long,Long>, Boolean>>(tupleOfCartesian._1, doesDominate(tupleOfCartesian._2)));

		//print target:76 user1:152
		dominanceInfo.filter(e->e._1 == 76 && e._2._1._1==152).foreach(e->System.out.println("Target: " + e._1 +
				" Others: " + e._2._1._1 + " , " + e._2._1._2 +
				" 1Dominates2: " + e._2._2));

		// select non-dominated(neighbor) users for each target seen in targetUsersFiltered
		JavaPairRDD<Long, Long> neighborsTemp = selectNeighbors(dominanceInfo,candidateUsers);

		// print
		neighborsTemp.filter(e->e._1 == 76).foreach(e->System.out.println("NeighborsTemp for 76: " + e._1 + " " + e._2));

		return neighborsTemp;

	}


	/**
	 * convert list<matrixEntry> to (targetUser,otherUser) -->FeatureSim pairs (for all type of data)
	 * @param simList: list of MatrixEntry indicating similarity among users
	 * @return
	 */
	private JavaPairRDD<Tuple2<Long, Long>, FeatureSim> createSimPairs(
			List<JavaRDD<MatrixEntry>> simList) {
		JavaPairRDD<Tuple2<Long,Long>,FeatureSim> allSimPairs = null;
		for(int i=0; i<simList.size(); i++){
			JavaRDD<MatrixEntry> featureBasedSim = simList.get(i);
			int index = i;
			JavaPairRDD<Tuple2<Long,Long>,FeatureSim> simMap = featureBasedSim.mapToPair(
					entry->new Tuple2<Tuple2<Long,Long>,FeatureSim>(
							new Tuple2<Long,Long>(entry.i(),entry.j()), new FeatureSim(index, entry.value())));

			//TODO is there a better way to do this?
			if(allSimPairs == null){
				allSimPairs = simMap;
			} else {
				allSimPairs.union(simMap);
			}
		}

		return allSimPairs;
	}


	/**
	 * Find out if userId is already selected as neighbor
	 * Cases:
	 * 1) At neighborsBCValue: neighbor value is equal to null: Not neighbor
	 * 2) Entry.u1 == neighborId : Neighbor
	 * 3) Entry.u2 ==neighborId : Neighbor
	 * 
	 * @param neighborsBCValue: target->neighborId
	 * @param entry: target->(u1,u2,1dominates2) info
	 * @return
	 */
	private boolean isNeighbor(Broadcast<Map<Long, Long>> neighborsBC,
			Tuple2<Long, Tuple2<Tuple2<Long, Long>, Boolean>> entry) {
		boolean retVal = false;

		if(neighborsBC.value().get(entry._1) != null){
			if(neighborsBC.value().get(entry._1) == entry._2._1._1){
				// this entry is belong to an already selected neighbor
				retVal = true;
			} else if(neighborsBC.value().get(entry._1) == entry._2._1._2){
				// this entry is belong to an already selected neighbor
				retVal = true;
			}
		}

		return retVal;
	}


	private JavaPairRDD<Long, Long> selectNeighbors(
			JavaPairRDD<Long, Tuple2<Tuple2<Long, Long>, Boolean>> dominanceInfo, 
			JavaPairRDD<Long, Long> candidateUsers) {

		// create 'dominated' users list for each target user
		JavaPairRDD<Long,Long> dominatedUsers = dominanceInfo
				.mapToPair(e->new Tuple2<Long,Long>(e._1, findDominated(e._2)))
				.filter(e->e._2 != null);

		//print target:76 
		//dominatedUsers.filter(e->e._1 == 76).foreach(e->System.out.println("Dominated: " + e._1 + " " + e._2));

		// find candidateUsers - dominatedUsers for each target TODO What if subtraction return less than N users??
		JavaPairRDD<Long, Long> nonDominateds = candidateUsers.subtract(dominatedUsers);

		//print target:76 
		//System.out.println("#Neigbors: " +neighbors.filter(e->e._1 == 76).count());
		//neighbors.filter(e->e._1 == 76).foreach(e->System.out.println("Neigbors: " + e._1 + " " + e._2));

		return nonDominateds;
	}


	/**
	 * Find if user2 is dominated by user1
	 * Do not consider the user1's in neighbor list
	 * @param neighbors: Already selected neighbors format (targetUser, user1)
	 * @param entry: Format is (user1,user2)-->Dominates/Not
	 * @return dominatedUserId or null(if not dominated)
	 */
	private Long findDominated(Tuple2<Tuple2<Long, Long>, Boolean> entry) {
		Long dominatedUserId = null;

		if(entry._2 == true){
			dominatedUserId = entry._1._2;
		}


		return dominatedUserId;
	}


	private Tuple2<Tuple2<Long,Long>, Boolean> doesDominate(
			Tuple2<Tuple2<Long, Iterable<FeatureSim>>, Tuple2<Long, Iterable<FeatureSim>>> cartesianOfSimilarity) {		
		Long user1Id = cartesianOfSimilarity._1._1;
		Long user2Id = cartesianOfSimilarity._2._1;

		Boolean dominance = doesDominate( cartesianOfSimilarity._1._2,  cartesianOfSimilarity._2._2);
		Tuple2<Tuple2<Long,Long>, Boolean> dominanceInfo = new Tuple2<Tuple2<Long,Long>, Boolean>(
				new Tuple2<Long, Long>(user1Id,  user2Id), dominance);
		return dominanceInfo;
	}


	/**
	 * control if neighbor1 dominates neighbor2
	 * @param neighbor1SimList: neighbor1's simlist to the target user
	 * @param neighbor2SimList: neighbor2's simlist to the target user
	 * @return
	 */
	private Boolean doesDominate(Iterable<FeatureSim> user1FeatureSims, Iterable<FeatureSim> user2FeatureSims) {
		Boolean  retVal  = true;

		// create hashmap for user2FeatureSims to make search eaiser
		HashMap<Integer, Double> user2FeaturesMap = new HashMap();
		for(FeatureSim f2:user2FeatureSims){
			user2FeaturesMap.put(f2.getFeatureId(), f2.getCosSim());
		}

		// compare user1FeatureSims and user2FeatureSims
		int countLarger = 0;
		for(FeatureSim f1:user1FeatureSims){
			Integer fId = f1.getFeatureId();

			Double simVal1 = f1.getCosSim();
			Double simVal2 = user2FeaturesMap.get(fId);

			if(simVal2 == null){
				// no such similarity in between user2 and target user
				System.out.println("Warning: Might be error. No such similarity in between user2 and target user");
			} else if(simVal1 > simVal2){
				countLarger++;
				// follows the dominance rule
			} else if(simVal1 == simVal2){
				// follows the dominance rule
			} else if(simVal1 < simVal2){
				// violates the dominance rule
				retVal  = false;
				break;
			}

		}

		if(retVal == true && countLarger < 1){
			// violates the dominance rule
			retVal  = false;
		}


		return retVal;
	}


	/**
	 * TODO Copied from UserBasedCollabfiltering--> need a better design!!
	 * @param simEntries: similarity among users
	 * @return matrix entry for most similar N users + + the ones that have the equal sim value to the last one
	 */
	private JavaRDD<MatrixEntry> selectMostSimilars(JavaRDD<MatrixEntry> simEntries) {
		// Create sorted list (based on similarity) of other users for each user	
		// sort by value and group by i 
		JavaRDD<MatrixEntry> sortedSimEntriesUnionRdd = simEntries.sortBy(x->x.value(),false,1);
		JavaRDD<Iterable<MatrixEntry>> groupedSortedSimUnion = sortedSimEntriesUnionRdd.groupBy(m->m.i()).values();
		//groupedSortedSimUnion.foreach(entry->print(entry));

		// Select most similar N entries 
		JavaRDD<MatrixEntry> topN = groupedSortedSimUnion.flatMap((Iterable<MatrixEntry> eList)->Utils.getTopNPlusEquals(N, eList));
		// print top-k
		//topK.foreach(entry->System.out.println(entry.toString()));

		return topN;
	}


	/**
	 * TODO: Copied from UserBasedCollabFiltering, I should find a better design!!
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


		// create itemid-->userid list
		JavaPairRDD<Integer, Iterable<Integer>> invertedIndexGrouped = invertedIndexMapped.groupByKey();
		//invertedIndexGrouped.foreach(t->printTuple2(t));

		JavaRDD<Iterable<Integer>> values = invertedIndexGrouped.values();
		// for each rdd(~entry) find the freq. of users
		retVector = values.map(uList-> Utils.countVals(largestUserId+1, uList));

		return retVector;
	}

}