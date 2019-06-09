package com.learn.spark.SparkDataFrame

import com.learn.spark.utils.Context

object DataFrameProcessing extends App with Context{


  // We are utilizing Seattle Library Collection data set from kaggle for exploring Spark Data frame operations
  // Create a DataFrame from reading a CSV file
  val dfLib = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/LCI.csv")
    .toDF("BibNum","Title", "Author", "ISBN", "PublicationYear", "Publisher", "Subjects", "ItemType", "ItemCollection", "FloatingItem", "ItemLocation", "ReportDate", "ItemCount")

  // Display first 25 rows
  dfLib.show(25)

  // Print the schema
  dfLib.printSchema()

  // Display the select columns for the first 10 rows
  dfLib.select("BibNum","Title","Author","ISBN").show(10)

  // Filter rows with a given expression and display them
  dfLib.filter("Author == 'Kirkman, Robert'").show()

  // Filter rows from data frame using Like in conditional expression
  dfLib.filter("Author like 'Olson%'")
    .show(10)

  // Chaining of filters
  println(s"Chaining of multiple filters example...")
  dfLib.filter("Author like 'Olson%'")
    .filter("Publisher == 'Oak Lane Press,' or PublicationYear='2015'")
    .show(10)

  println(s"IN Clause example...")
  // IN Clause
  dfLib.filter("BibNum IN (1736505,3018388,3265336)").show()

  println(s"Group by Author...")
  // Group By
  dfLib.groupBy("Author").count().show()

  println(s"Group by Author and display only count > 15 by chaining a filter")
  // Group By with filter
  dfLib.groupBy("Author").count().filter("count > 15").show()

  println(s"Order by Author..")

  //Order By example
  dfLib.groupBy("Author").count().filter("count > 15").orderBy("Author").show(20)

  sparkSession.stop()


}
