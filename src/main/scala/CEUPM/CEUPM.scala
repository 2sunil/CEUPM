package CEUPM

import org.apache.spark.util.{ BoundedPriorityQueue, Utils }
import scala.io.Source
import java.io._
import scala.collection.mutable._
import util.control.Breaks._
import scala.util._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.rdd._
import org.mee.sparkk._

object CEUPM {

  def partitioning(items: List[Int], p: Int, r: Int): Map[Int, Int] = {
    var gitems = Map[Int, Int]()
    var i = 0
    var node = 0
    for (k <- 1 to p) {
      for (kk <- 0 until k) {
        gitems.put(items(i), node)
        i += 1
      }
      node += 1
    }
    if (r != 0) {
      for (kk <- 0 until r) {
        gitems.put(items(i), node)
        i += 1
      }
      node += 1
    }
    gitems
  }

  def main(args: Array[String]) {
    val mutil = args(1).toDouble  // minimum utility threshold
    val Mpar = args(2).toInt
    val merge = args(3).toBoolean // 1 : Merge, 0: Without Merge
    val eucs = args(4).toBoolean  // 1 : With EUCS optimization, 0 : Without EUCS  
    
    var par = 0
    var st = System.currentTimeMillis()

    val conf = new SparkConf().setAppName("CEUPM")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.mee.sparkk.ARegistrator")
      .set("spark.storage.blockManagerHeartBeatMs", "30000000")
      .set("spark.eventLog.enabled", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    var ti = sc.textFile(args(0)).repartition(Mpar)

    val trans = ti.map(s => new Transaction(s))
    trans.persist()

    val mutilBroad = sc.broadcast(mutil)

    val twu = trans.flatMap(x => x.itemset.map(y => (y._1, x.utility)))
      .reduceByKey(_ + _)
      .filter(x => x._2 >= mutilBroad.value)
      .collect()
      .toMap

    val twuBroad = sc.broadcast(twu)
    val revisedTransaction = trans.map(t => {
      t.itemset = t.itemset.filter(x => twuBroad.value.get(x._1) != None)
      Sorting.quickSort(t.itemset)(Ordering[(Int, Int)].on(x => (twuBroad.value.get(x._1).get, x._1)))
      t
    }).filter(x => x.itemset.size >= 1)
    revisedTransaction.persist()
    trans.unpersist(false)

    val items = twu.toList.sortBy(x => x._2).map(x => x._1)

    var n = items.size
    var p = 0
    var r = 0
    var g = 0
    var ii = 1
    while (g <= n) {
      g = ii * (ii + 1) / 2
      ii = ii + 1
    }
    if (g == n) {
      p = ii
    } else {
      p = ii - 2
      var gg = p * (p + 1) / 2
      r = n - gg
    }
    if (r == 0) {
      par = p
    } else {
      par = p + 1
    }

    val grouped_items = partitioning(items, p, r)
    val grouped_itemsBroad = sc.broadcast(grouped_items)

    if (merge == 1) {

      var kset = revisedTransaction.flatMap { x =>
        {
          var tlist = ArrayBuffer[(Int, Array[(Int, Int)])]()
          var added = Map[Int, Boolean]()
          for (i <- 0 to x.length - 1) {
            var firstitem = x.itemset(i)._1
            var gid = grouped_itemsBroad.value(firstitem)
            if (added.get(gid) == None) {
              tlist.append((gid, x.itemset.slice(i, x.length)))
              added(gid) = true
            }
          }
          tlist.iterator
        }
      }
      kset.persist()
      revisedTransaction.unpersist(false)

      kset = kset.partitionBy(new BinPartitioner(par))

      val gset = kset.groupByKey()

      val mrgBroad = sc.broadcast(merge)
      val eucsBroad = sc.broadcast(eucs)
      val maps = gset.flatMap(x => {
        var itemToEx = items.intersect(grouped_items.groupBy(f => f._2)(x._1).keys.toList)
        var itemToCul = items.slice(items.indexOf(itemToEx(0)), items.size)

        var hm = new ExploreHUP(itemToCul, twuBroad.value, mrgBroad.value, eucsBroad.value)
        for (transac <- x._2) {
          hm.transToCUL(transac)
        }

        hm.miner(mutilBroad.value.toInt, grouped_itemsBroad.value, x._1)
      })

      val fmaps = maps.collect().toMap

      var et = System.currentTimeMillis()
      println("CEUPM: ")
      println("HUPs: " + fmaps.size)
      println("Running time: " + (et - st))

      sc.stop()
    } else {
      println("Error:")
    }

  }
}