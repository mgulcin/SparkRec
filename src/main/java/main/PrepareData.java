package main;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Divide input data into few chunks which are going to be used as stream data
 * @author makbule.ozsoy
 *
 */
public class PrepareData {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6106269076155338045L;
	public static String logPath;
	public static JavaSparkContext sc;

	public static Integer chunkSize = 20;

	public static void main(String[] args) {

		if(args.length != 4){
			System.out.println("Wrong number of arguments. Give a file as input.");
			System.exit(-1);
		}

		// file for log
		logPath = args[0];

		// some file in the form of userId,<comma separated list of itemIds> e.g. u1,i1,i32,i36,i94
		String trainFile = args[1];
		String testFile = args[2];
		String outputDir = args[3];

		SparkConf conf = new SparkConf().setAppName("Chunks Application").setMaster("local[2]").set("spark.executor.memory","1g");
		sc = new JavaSparkContext(conf);

		// create chunks
		String suffix = "trainData_";
		createChunks(trainFile, outputDir, suffix);
		//createChunks(testFile, outputDir);-->seem unnecessary


		sc.close();
	}

	private static void createChunks(String file, String outputDir, String suffix) {
		// read data from file to userid, itemid e.g. u3-->i21,u3-->i45, u3-->i89
		JavaPairRDD<Integer, Integer> dataFlattened = Utils.readData(sc, file);

		// create chunks and write chunks to files
		partitionData(dataFlattened, chunkSize, outputDir, suffix);

	}

	/**
	 * For each user there will be chunkSize many input (non-duplicate)
	 * @param data
	 * @param chunkSize
	 * @param outputDir 
	 * @return
	 */
	private static void partitionData(
			JavaPairRDD<Integer, Integer> data, Integer chunkSize, String outputDir, String suffix) {

		// get each user
		JavaRDD<Integer> user = data.map(e->e._1).distinct();

		// get data for each user
		JavaPairRDD<Integer, Iterable<Integer>> dataByKey = data.groupByKey();

		// write data per user 
		dataByKey.foreach(e->writeToFile(e, outputDir, suffix, chunkSize));


		/*//TODO find a better way than using groupBykey
		JavaPairRDD<Integer, Iterable<Integer>> dataByKey = data.groupByKey();
		JavaPairRDD<Integer, List<Iterable<Integer>>> chunksByKey = dataByKey.mapToPair(tuple->new Tuple2<Integer, List<Iterable<Integer>>>(tuple._1,createChunks(tuple._2,chunkSize)));
		JavaPairRDD<Integer, Iterable<Integer>> flattedChunksByKey = chunksByKey.flatMapValues(e->e);*/

	}

	private static void writeToFile(Tuple2<Integer, Iterable<Integer>> entry,
			String outputDir, String suffix, Integer chunkSize) {
		Integer targetUser = entry._1;
		Iterable<Integer> itemList = entry._2;

		// write chunks to files
		int index = 1;
		int count = 0;
	
		for(Integer item: itemList){
			count++;
			
			String path = outputDir +"\\" + suffix + index+".csv";
			Printer.printToFile(path, targetUser + " , " + item);
			
			if((count % chunkSize) == 0){
				index++;
			}
		}
		
	}

}

