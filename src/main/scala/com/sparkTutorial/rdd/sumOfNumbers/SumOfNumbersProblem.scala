package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */

    val conf = new SparkConf().setAppName("sum-100-primes").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val primesFile = sc.textFile("in/prime_nums.text")

    val numbers = primesFile.flatMap(line => line.split( "\\s+" ))

    val validNumbers = numbers.filter(number => !number.isEmpty)

    val primes = validNumbers.map(string => string.toInt)

    val result = primes.take(100).reduce((x,y) => x + y)

    println("Total sum " + result)
  }
}
