package main.scala.com.bowan.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max

/** Find the minimum temperature by weather station */
object MaxPrecipitation {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val date = fields(1)
    val entryType = fields(2)
    //val prcp = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    val prcp = fields(3).toFloat
    (stationID, (date, entryType, prcp))
  }
  
  def onlyStation(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    (stationID)
  }
    
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")
    
    // Read each line of input data
    val lines = sc.textFile("../sample-data/1800.csv")
    
    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)
    
    // Filter out all but TMIN entries
    val prcps = parsedLines.filter(x => x._2._2 == "PRCP")
    
    // Convert to (stationID, prcp)
    val stationPrcp = prcps.map(x => (x._1, (x._2._1, x._2._3)))
 
    //stationPrcp.take(10).foreach(println)
    
    
    val checkStations = stationPrcp.map((x => (x._1)))
    checkStations.distinct.collect().foreach(println)
    
     
    // Reduce by stationID retaining the minimum temperature found
    val maxP = stationPrcp.reduceByKey((x, y) => if (x._2 > y._2) x else y)
    
    maxP.take(10).foreach(println)
    
 
    // Collect, format, and print the results
    val results = maxP.collect()
    
    for (result <- results.sorted) {
       val station = result._1
       val date = result._2._1
       val prcp = result._2._2
       // = f"$temp%.2f F"
       println(s"Station $station max prcp is $prcp which occured on date $date") 
    }
      
  }
}