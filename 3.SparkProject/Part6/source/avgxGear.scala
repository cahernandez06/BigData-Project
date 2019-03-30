package avgxGear

import java.io._

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark._
import org.apache.spark.sql.functions._

import scala.collection.mutable

import scala.reflect.runtime.universe
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.immutable.ListMap

object SimpleApp {

case class Pair(x: Double, y: Double = 0)

case class TempRec(Id: String, Ozone: String,	SolarR: String,	Wind: String,	Temp: String,	Month: String,	Day: String)


def main(args: Array[String]) {
	val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
			val sc = new SparkContext(conf)
			val sqlContext = new org.apache.spark.sql.SQLContext(sc)
			import sqlContext.implicits._

			//STEP 1: Dataset choosen: airquality to check temperature by month
			val csv = sc.textFile("/home/cloudera/Desktop/BigData/airquality.csv")  

			val headerAndRows = csv.map(line => line.split(",").map(_.trim))
			val header = headerAndRows.first
			val data = headerAndRows.filter(_ (0) != header(0)) //No header
			val numBs = data.count()
			println(s"Lines in file airquality.csv: $numBs")

			//STEP 2: Categorical variable: p(5)="Month" and Numeric variable: p(4)="Temp" 
			val population = data
			.map(p => Pair(p(5).toDouble, p(4).toDouble))
			.toDF()

			//STEP 3: Compute mean and variance of each category 
			population.groupBy("x").agg(mean("y") as "Mean",variance("y") as "Variance").show() 


			//STEP 4: Sample for bootstrapping of 25%
			val sampl = population.sample(false,0.25)
			//    sampl.foreach(println)

			//STEP 5: 1000 iterations
			val iterations = 10
			val avgMap: mutable.Map[String, (Double, Double)] = mutable.HashMap()
			for (i <- 0 until iterations){
				//STEP 5a: Resample data with 100% and replace
				val reSample = sampl.sample(true,1)
						//STEP 5b: Calculate mean and std. dev.
						val res = reSample.groupBy("x").agg(mean(population("y")), variance(population("y")))
						//STEP 5c: Running sum
						val result = res.rdd.map(x => (x.get(0).toString, (x.get(1).toString.toDouble, x.get(2).toString.toDouble)))
						.collect().toMap

						avgMap ++= result.map { case (a, b) => {
							val s1 = avgMap.getOrElse(a, (0D, 0D))._1 + (if (b._1.isNaN) 0D else b._1)
									val s2 = avgMap.getOrElse(a, (0D, 0D))._2 + (if (b._2.isNaN) 0D else b._2)
									a -> (s1, s2)
						}
				}
			}
	println("RESULTS")
	//STEP 6: Divide by 1000 and print results
	val res2 = avgMap.map(x => (x._1, x._2._1/iterations, x._2._2/iterations)).toList
	res2.toDF("Period", "Mean", "Variance").show()

	//STEP 7 Determine the absolute error percentage for each of the values being estimated. 
	val temp =population.groupBy("x").agg(mean("y"),variance("y"))
	val toSort = temp.rdd.map(x => (x.get(0).toString, (x.get(1).toString.toDouble, x.get(2).toString.toDouble))).collect().toMap
	val actual = toSort.toSeq.sortBy(_._1)
	//			actual.foreach(x => println (x._1, (x._2._1, x._2._2)))
	val temp3 = avgMap.map(x => (x._1, (x._2._1/iterations, x._2._2/iterations))).toList

	val estimate = temp3.toSeq.sortBy(_._1)
	val totalMap: mutable.Map[String, (Double, Double, Double)] = mutable.HashMap()
	for (i <- 0 until actual.length){
		println(actual(i)+ " valor "+ estimate(i))
		val mean = ((Math.abs(actual(i)._2._1 - estimate(i)._2._1)*100)/actual(i)._2._1)
		val variance = ((Math.abs(actual(i)._2._2 - estimate(i)._2._2)*100)/actual(i)._2._2)
		totalMap += (actual(i)._1 -> (mean,variance,0.25))
	}
	println("STEP OPTIONAL")
	val abs = totalMap.map(x => (x._1, x._2._3, x._2._1, x._2._2)).toList
	abs.toDF("Period","Percentage", "Mean", "Variance").show()
}
}