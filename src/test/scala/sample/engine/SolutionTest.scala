package sample.engine

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import scala.reflect.io.File

@RunWith(classOf[JUnitRunner])
class SolutionTest extends AnyFunSuite {
  test("TDC Tasks") {

    Part1Main.main(Array())
    Part2Main.main(Array())

  }
}
