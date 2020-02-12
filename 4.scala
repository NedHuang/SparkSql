package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

class ConfQ4(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean](descr = "text", required = false)
  val parquet = opt[Boolean](descr = "parquet", required = false)
  verify()
}

// select n_nationkey, n_name, count(*) from lineitem, orders, customer, nation
// where
//   l_orderkey = o_orderkey and
//   o_custkey = c_custkey and
//   c_nationkey = n_nationkey and
//   l_shipdate = 'YYYY-MM-DD'
// group by n_nationkey, n_name
// order by n_nationkey asc;

object Q4{
    val log = Logger.getLogger(getClass().getName())
    def main(argv: Array[String]) {
		val args = new ConfQ4(argv)

		log.info("Input: " + args.input())
		log.info("Date: " + args.date())
        log.info("Input: " + args.text())
		log.info("Date: " + args.parquet())

		val conf = new SparkConf().setAppName("Q4")
		val sc = new SparkContext(conf)
		val date = args.date()
        
        if(args.text()){
            val lineItemTextFile = sc.textFile(args.input() + "/lineitem.tbl")
            val customerTextFile = sc.textFile(args.input() + "/customer.tbl")
            val ordersTextFile = sc.textFile(args.input() + "/orders.tbl")
            val nationTextFile = sc.textFile(args.input() + "/nation.tbl")

            val lineItems = lineItemTextFile
                // (l_ORDERKEY, 1(which is counter))
                .filter(line =>(line.split('|')(10).contains(date)))
                // .map(line => ((line.split('|')(0).toInt,line.split('|')(10)),1 ))
                .map(line => (line.split('|')(0).toInt,1 ))

            val orders = ordersTextFile
                // (O_ORDERKEY, O_CUSTKEY)
                .map(line=>(line.split('|')(0).toInt, line.split('|')(1).toInt))

            val customer = customerTextFile
                // (C_CUSTKEY, C_NATIONKEY)
                .map(line=>(line.split('|')(0).toInt, line.split('|')(3).toInt))

            val nation = nationTextFile
                // (N_NATIONKEY, n_name)
                .map(line =>(line.split('|')(0).toInt, line.split('|')(1)))

            // val ordersBroadcast = sc.broadcast(orders.collectAsMap())
            val customerBroadcast = sc.broadcast(customer.collectAsMap())
            val nationBroadcast = sc.broadcast(nation.collectAsMap())

            // val ordersMap = ordersBroadcast.value
            val customerMap = customerBroadcast.value
            val nationMap = nationBroadcast.value
            val record = lineItems.cogroup(orders)
                .filter(p => p._2._1.iterator.hasNext)
                .map(p => (customerMap(p._2._2.iterator.next()), p._2._1.iterator.next()))
                .reduceByKey(_ + _)
                .map(p => (p._1.toInt, (nationMap(p._1), p._2)))
                .sortByKey()
                .collect()
                .foreach(p => println("(" + p._1 + "," + p._2._1 + "," + p._2._2 + ")"))
        }
        else if(args.parquet()){
            val sparkSession = SparkSession.builder.getOrCreate
            val lineItemFileDF = sparkSession.read.parquet(args.input() + "/lineitem")
            val customerFileDF = sparkSession.read.parquet(args.input() + "/customer")
            val ordersFileDF = sparkSession.read.parquet(args.input() + "/orders")
            val nationFileDF = sparkSession.read.parquet(args.input() + "/nation")

            val lineitemRdd = lineItemFileDF.rdd
            val customerRdd = customerFileDF.rdd
            val ordersRdd = ordersFileDF.rdd
            val nationRdd = nationFileDF.rdd

            val lineItems = lineitemRdd
                // (l_ORDERKEY, 1 (which is counter))
                .filter(line =>(line.getString(10).contains(date)))
                .map(line => (line.getInt(0),1 ))

            val orders = ordersRdd
                // (O_ORDERKEY, O_CUSTKEY)
                .map(line=>(line.getInt(0), line.getInt(1)))

            val customer = customerRdd
                // (C_CUSTKEY, C_NATIONKEY)
                .map(line=>(line.getInt(0), line.getInt(3)))

            val nation = nationRdd
                // (N_NATIONKEY, n_name)
                .map(line =>(line.getInt(0), line.getString(1)))

            // val ordersBroadcast = sc.broadcast(orders.collectAsMap())
            val customerBroadcast = sc.broadcast(customer.collectAsMap())
            val nationBroadcast = sc.broadcast(nation.collectAsMap())

            // val ordersMap = ordersBroadcast.value
            val customerMap = customerBroadcast.value
            val nationMap = nationBroadcast.value
            val record = lineItems.cogroup(orders)
                .filter(p => p._2._1.iterator.hasNext)
                .map(p => (customerMap(p._2._2.iterator.next()), p._2._1.iterator.next()))
                .reduceByKey(_ + _)
                .map(p => (p._1.toInt, (nationMap(p._1), p._2)))
                .sortByKey()
                .collect()
                .foreach(p => println("(" + p._1 + "," + p._2._1 + "," + p._2._2 + ")"))
            
        }
    }
}
