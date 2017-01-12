package com.spnotes.spark

import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by sunilpatil on 4/19/16.
  */
object JDBCRDDClient {

  case class Address(addressId: Int, contactId: Int, line1: String, city: String, state: String, zip: String)

  def main(argv: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HelloJDBC")
    val sparkContext = new SparkContext(sparkConf)

    val jdbcRdd = new JdbcRDD(sparkContext, getConnection,
      "select * from address limit ?,?",
      0, 5, 1, convertToAddress)

    jdbcRdd.foreach(println)
  }

  def getConnection(): Connection = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost/test1?" + "user=test1&password=test1")
  }

  def convertToAddress(rs: ResultSet): Address = {
    new Address(rs.getInt("addressid"), rs.getInt("contactid"),
      rs.getString("line1"), rs.getString("city"), rs.getString("state"),
      rs.getString("zip"))
  }
}
