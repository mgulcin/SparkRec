
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
























import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;



























import com.google.common.base.Optional;
import com.google.common.collect.Iterables;



























import scala.Tuple2;

public class Main implements Serializable {
	public static void main(String[] args) {

		if(args.length !=1){
			System.out.println("Wrong number of arguments. Give a file as input.");
			System.exit(-1);
		}

		// some file in the form of userId,<comma separated list of itemIds> e.g. u1,i1,i32,i36,i94
		String file = args[0];
		
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g");
		JavaSparkContext sc = new JavaSparkContext(conf);

	
		// perform user based collaborative filtering
		/*UserBasedCollabFiltering ucf = new UserBasedCollabFiltering();
		ucf.performCollaborativeFiltering(file);*/
		
		//UserBasedCollabFiltering.performCollaborativeFiltering(sc, file);
		//ItemBasedCollabFiltering.performCollaborativeFiltering(sc, file);
		HybridRec.performCollaborativeFiltering(sc, file);
		
		sc.close();
	}

}


