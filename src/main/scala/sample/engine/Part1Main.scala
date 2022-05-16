package sample.engine

import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object Part1Main {

  def main(args: Array[String]) : Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Solution").getOrCreate()
    val csvRdd = Try(getClass.getClassLoader.getResource("csv/groceries.csv").getPath) match {
      case Success(path) => Try(spark.read.textFile(path)) match {
        case Success(csvDf) => csvDf
        case Failure(csvDFException) => throw new Exception(s"Failed to read CSF from $path. ${csvDFException.getMessage}", csvDFException)
      }
      case Failure(exception) => throw new Exception(s"Failed to load csv file. ${exception.getMessage}", exception)
    }

    Part1TaskRunner.task1PrintFirst5(csvRdd)

    val distinctProductsDS = Part1TaskRunner.task2FindDistinctProducts(csvRdd, spark)

    Part1TaskRunner.task3CountDistinctProducts(distinctProductsDS)

  }

}
