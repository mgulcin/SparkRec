package eval;
import org.apache.spark.api.java.JavaPairRDD;


public class Evaluate {

	public static EvaluationResult evaluate(JavaPairRDD<Integer, Integer> output,
			JavaPairRDD<Integer, Integer> testData) {
		
		// compare output to testData, create EvaluationResult
		JavaPairRDD<Integer, Integer> intersection = output.intersection(testData);
		double tp = intersection.count();
		double fp = output.count() - tp;
		double fn = testData.count() - tp;
		double tn = 0;
		
		EvaluationResult evalResult = new EvaluationResult(tp, fp, tn, fn);		
		return evalResult;
	}
	
}
