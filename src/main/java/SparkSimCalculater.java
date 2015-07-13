
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.AbstractJavaRDDLike;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

public class SparkSimCalculater {
	public static void main(String[] args) {
		
		if(args.length !=1){
			System.out.println("Wrong number of arguments. Give a file as input.");
			System.exit(-1);
		}
		
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g");;
		JavaSparkContext sc = new JavaSparkContext(conf);

		// some file in the form of userId,<comma separated list of itemIds> e.g. u1,i1,i32,i36,i94
		String file = args[0];

		// load and parse data
		JavaRDD<String> data = sc.textFile(file);
		//JavaRDD<IndexedRow> parsedData = loadAndParseData(sc, data);

		// create inverted index representation: itemId to userId list e.g. i32--><u1,u5,u89>
		JavaPairRDD<String, String> invertedIndexMapped = invertedIndexMap(data);
		JavaPairRDD<String, Iterable<String>> invertedIndexGrouped = invertedIndexGroup(invertedIndexMapped);

		// print inverted list
		//print(invertedIndexGrouped);

		// calculate cosine similarity of users
		JavaRDD<Vector> vectorOfUsers = createVectorOf(invertedIndexGrouped);
		RowMatrix matrix = new RowMatrix(vectorOfUsers.rdd());

		/*// Get its size. -- just to control
		long m = matrix.numRows();
		long n = matrix.numCols();
		System.out.println(m + " , "+ n);*/


		// Compute similar columns perfectly, with brute force.
		CoordinateMatrix simsPerfect = matrix.columnSimilarities();

		/*// Get its size. -- just to control
		long mSim = simsPerfect.numRows();
		long nSim = simsPerfect.numCols();

		System.out.println(mSim + " , "+ nSim);
		 */		

		// print similarities
		 RDD<MatrixEntry> simEntriesRdd = simsPerfect.entries();
		 print(simEntriesRdd);

		

	}

	private static void print(RDD<MatrixEntry> simEntriesRdd) {
		simEntriesRdd.toJavaRDD().foreach(new VoidFunction<MatrixEntry>() {

			@Override
			public void call(MatrixEntry entry) throws Exception {

				System.out.println(entry.i() + " , " + entry.j() + " , " + entry.value());
			}

		});

	}

	private static JavaRDD<Vector> createVectorOf(
			JavaPairRDD<String, Iterable<String>> data) {
		JavaRDD<Vector> retVector = null;
		JavaRDD<Iterable<String>> values = data.values();

		retVector = values.map(
				new Function<Iterable<String>, Vector>() {

					@Override
					public Vector call(Iterable<String> strList) throws Exception {
						List<Integer> indices = new ArrayList<Integer>();
						List<Double> values = new ArrayList<Double>();
						for (String str:strList){
							indices.add(Integer.parseInt(str));
							values.add(1.0);
						}

						// convert List<Integer> to int[] - PS I love Java8 :)
						int size = 8500;// how to give dynamic size, whoch I do nt know beforehand??
						int[] indicesArray = indices.stream().mapToInt(i->i).toArray();
						double[] valuesArray = values.stream().mapToDouble(i->i).toArray();

						Vector sv = Vectors.sparse(size, indicesArray, valuesArray);

						return sv;
					}
				}
				);

		return retVector;
	}

	@SuppressWarnings("serial")
	private static void print(
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


	public static JavaPairRDD<String, String> invertedIndexMap(JavaRDD<String> data) {		
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

	public static JavaPairRDD<String, Iterable<String>> invertedIndexGroup(JavaPairRDD<String, String> data) {		
		JavaPairRDD<String, Iterable<String>> inverted = data.groupByKey();
		return inverted;

	}

	/**
	 * TODO read this too: https://gist.github.com/vrilleup/9e0613175fab101ac7cd
	 * https://spark.apache.org/docs/latest/mllib-clustering.html
	 * @return sparse vector
	 */
	public static JavaRDD<IndexedRow> loadAndParseData(JavaSparkContext sc,
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


