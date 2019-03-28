// Databricks notebook source
sc //press ctrl+enter

// COMMAND ----------

val EmployeeFile = spark.read.format("csv").option("header","true").load("/FileStore/tables/emp.csv");

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

EmployeeFile.groupBy("mid").agg(count("mid")).show

// COMMAND ----------

val color = "red"

// COMMAND ----------

val f = "s"
displayHTML(s"""
<svg width="100" height="100">
   <circle cx="50" cy="50" r="40" stroke="green" stroke-width="4" fill=${color} />
   Sorry, your browser does not support inline SVG.
</svg>
<h1>heading</h1>
""")
