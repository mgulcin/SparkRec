package main;
import recommender.FeatureSim;
import scala.Tuple2;


public class Printer {
	public static<T> void printTupleWithIterable(Tuple2<T, Iterable<T>> t){
		System.out.print(t._1() + " , ");

		for(T tVal:t._2){
			System.out.print(tVal + " , ");
		}
		System.out.println();
	}

	public static<T> void printTupleWithTuple(Tuple2<T, Tuple2<T,T>> t){
		System.out.print(t._1() + " , ");
		System.out.print(t._2()._1() + " , ");
		System.out.print(t._2()._2() + " , ");

		System.out.println();
	}

	public static<T> void printIterable(Iterable<T> t){
		for(T tVal:t){
			System.out.print(tVal.toString() + " , ");
		}
		System.out.println();
	}

	public static void printSimList(
			Tuple2<Long, Tuple2<Long, Iterable<FeatureSim>>> e) {
		System.out.print("Target: " + e._1.toString());
		Printer.printSims(e._2);
	}
	
	public static void printSims(Tuple2<Long, Iterable<FeatureSim>> e){
		System.out.print(" User: " + e._1.toString()
				+" SimList: " 
			); 
		if(e._2 != null){
			Printer.printIterable(e._2);
		} else {
			System.out.println("NULL");
		}
	}

	public static void printCartesianSimList(
			Tuple2<Long, Tuple2<Tuple2<Long, Iterable<FeatureSim>>, Tuple2<Long, Iterable<FeatureSim>>>> e) {
		System.out.println("Target: " + e._1.toString());
		Printer.printSims(e._2._1);	
		Printer.printSims(e._2._2);	
	}
}
