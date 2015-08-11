package main;
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
			System.out.print(tVal + " , ");
		}
		System.out.println();
	}
}
