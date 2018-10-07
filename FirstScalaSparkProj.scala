package SparkScalaPkg

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.io.Source

object FirstScalaSparkProj {
 def main(args: Array[String]): Unit = {
   var lst = List[Int] (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
   println("Sum of all values in the List is: " + lst.sum)
   println("Total number of elements in the List is: " + lst.length)
   println("Average of the values in the List is: " + (lst.sum:Double)/lst.length)
   println("Sum of all Even number in the List is: " + lst.filter(_ % 2 == 0).sum)
   println("Total number of elements divisible by 3 and 5 is: " +lst.filter(x => x % 3 == 0 && x % 5 == 0).length)
   println("Total number of elements divisible by 3 or 5 is: " +lst.filter(x => x % 3 == 0 || x % 5 == 0).length)
   var filePath = "C:\\Users\\Shivnath\\workspace\\SparkScalaTest\\src\\SparkScalaPkg\\TestFile1.txt"
   var src = Source.fromFile(filePath) // Create the sourcce
   val noOfLines = src.getLines.size //Get the no. of lines
   println("No of Lines in the input file is: " + noOfLines) //print output
   filePath = "C:\\Users\\Shivnath\\workspace\\SparkScalaTest\\src\\SparkScalaPkg\\TestFile1.txt" 
   src = Source.fromFile(filePath)
   val noOfWords = src.getLines().
   flatMap(_.split("\\W+")). // Regex function to identify WhiteSpace
   toList.                   // Convert to List
   groupBy((word: String) => word). //Group by words
   mapValues(_.length) // and count the no of occurence by using lenght
   println("No of Words in the input file is: " + noOfWords)
   filePath = "C:\\Users\\Shivnath\\workspace\\SparkScalaTest\\src\\SparkScalaPkg\\TestFile2.txt"
   src = Source.fromFile(filePath)
   val totalNoWords = (for {
      line <- src.getLines
    } yield {
      val words = line.split("-") //Split by "-"
      words.size
    }).sum //Get the sum of all words together in the file
   println("Total No of words after splitting by " + "'-'" + " in TestFile2 is: "+ totalNoWords)

 }
} 