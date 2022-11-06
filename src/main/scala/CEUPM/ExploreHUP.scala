package CEUPM

import scala.util._
import scala.collection.mutable._
import util.control.Breaks._
import scala.tools.nsc.doc.model.Public

class ExploreHUP {
  var twu: scala.collection.immutable.Map[Int, Int] = null

  var ItemCul: HashMap[Int, CUList] = HashMap()
  var itemToCul: scala.collection.immutable.List[Int] = null
  var tid = 1

  var hups = ArrayBuffer[(String, Double)]()

  var culs: ArrayBuffer[CUList] = ArrayBuffer()

  var FMAP: Map[Int, Map[Int, Int]] = new HashMap()

  var merging_flag: Boolean = false
  var eucs_flag: Boolean = false
  var p_laprune: Int = 0
  var p_cprune: Int = 0

  def this(itemToCul: scala.collection.immutable.List[Int], twu: scala.collection.immutable.Map[Int, Int], merging_flag: Boolean, eucs_flag: Boolean) {
    this()
    this.twu = twu
    this.merging_flag = merging_flag
    this.eucs_flag = eucs_flag
    this.itemToCul = itemToCul
    itemToCul.toList foreach {
      case (key) =>
        {
          var ul: CUList = new CUList(key)
          culs.append(ul)
          ItemCul.put(key, ul)
        }
    }
  }

  def transToCUL(t: Array[(Int, Int)]) {

    var HT: HashMap[ArrayBuffer[Int], Int] = HashMap()
    var ru: Int = 0
    var exTwu = 0
    var tx_key = ArrayBuffer[Int]()
    for (i <- t.length - 1 until -1 by -1) {
      var uu = t(i)

      exTwu += uu._2
      tx_key += uu._1
    }

    if (merging_flag) {
      if (!HT.contains(tx_key)) {
        HT.put(tx_key, ItemCul(t(t.size - 1)._1).elements.size)

        for (i <- t.length - 1 until -1 by -1) {
          val (key, value) = t(i)
          var ul: CUList = ItemCul(key)
          var element: E_CUL_List = new E_CUL_List(tid, value, ru, 0, 0)
          if (i > 0) {
            element.Ppos = ItemCul(t(i - 1)._1).elements.size
          } else {
            element.Ppos = -1
          }
          ul.addElement(element)

          ru += value
        }
      } else {
        var pos = HT.getOrElse(tx_key, 0)
        ru = 0
        for (i <- t.length - 1 until -1 by -1) {
          val (key, value) = t(i)
          var ul: CUList = ItemCul(key)
          ul.elements(pos).Nu += value
          ul.elements(pos).Nru += ru
          ul.sumNu += value
          ul.sumNru += ru
          ru += value
        }
      }

    } else {

      for (i <- t.length - 1 until -1 by -1) {
        val (key, value) = t(i)

        var ul: CUList = ItemCul(key)
        var element: E_CUL_List = new E_CUL_List(tid, value, ru, 0, 0)
        if (i > 0) {
          element.Ppos = ItemCul(t(i - 1)._1).elements.size
        } else {
          element.Ppos = -1
        }
        ul.addElement(element)
        ru += value
      }
    }

    if (eucs_flag) {
      for (i <- t.length - 1 until -1 by -1) {
        val (key, value) = t(i)
        var mapFMAPItemOpt = FMAP.get(key)

        var mapFMAPItem: Map[Int, Int] = null

        if (mapFMAPItemOpt.isEmpty) {
          mapFMAPItem = new HashMap[Int, Int]()
          FMAP.put(key, mapFMAPItem)
        } else {
          mapFMAPItem = mapFMAPItemOpt.get
        }

        for (j <- i + 1 until t.size) {
          var pairAfter = t(j)
          var twuSumOpt = mapFMAPItem.get(pairAfter._1)
          if (twuSumOpt.isEmpty) {
            mapFMAPItem.put(pairAfter._1, exTwu)
          } else {
            mapFMAPItem.put(pairAfter._1, twuSumOpt.get + exTwu)
          }
        }
      }
    }

    tid += 1
  }

  def miner(thresUtil: Int, glists: Map[Int, Int], gid: Int): Iterator[(String, Double)] = {

    for (i <- 0 until culs.size) {
      var ull: CUList = culs(i)
    }

    localMining(new Array[Int](0), culs, thresUtil, glists, gid)

    hups.iterator
  }

  def localMining(
    p:    Array[Int],
    CULs: ArrayBuffer[CUList], mutil: Int,
    gItems: Map[Int, Int], gid: Int) {

    for (i <- 0 until CULs.size) {
      var X = CULs(i)

      if (p.length != 0 || gItems(X.item) == gid) {
        if (X.sumNu + X.sumCu >= mutil) {
          hups.append((p.mkString(" ") + (if (p.isEmpty) "" else " ") + X.item, X.sumNu + X.sumCu))
        }

        if (X.sumNu + X.sumCu + X.sumNru + X.sumCru >= mutil) {

          var exULs = new ArrayBuffer[CUList]()

          exULs = construct(p.length + 1, X, CULs, i, mutil)
          var newPrefix = new Array[Int](p.length + 1)
          System.arraycopy(p, 0, newPrefix, 0, p.length)
          newPrefix(p.length) = X.item

          localMining(newPrefix, exULs, mutil, gItems, gid)
        }
      }
    }
  }

  def construct(length: Int, X: CUList, CULs: ArrayBuffer[CUList], st: Int, minUtility: Int): ArrayBuffer[CUList] = {

    var exCULs = new ArrayBuffer[CUList]()
    var ul: CUList = new CUList(X.item)
    var LAU: ArrayBuffer[Int] = ArrayBuffer()
    var CUTIL: ArrayBuffer[Int] = ArrayBuffer()
    var ey_tid: ArrayBuffer[Int] = ArrayBuffer()

    for (i <- 0 until CULs.size) {
      var uList: CUList = new CUList(CULs(i).item)
      exCULs.append(uList)
      LAU.append(0)
      CUTIL.append(0)
      ey_tid.append(0)
    }

    var sz: Int = CULs.size - (st + 1)
    var extSz = sz

    for (j <- (st + 1) until CULs.size) {
      if (eucs_flag) {
        var mapTWUFOpt = FMAP.get(X.item)

        if (!mapTWUFOpt.isEmpty) {
          var twuFOpt = mapTWUFOpt.get.get(CULs(j).item)
          if (!twuFOpt.isEmpty && twuFOpt.get < minUtility) {
            exCULs.update(j, null)
            extSz = sz - 1
          } else {
            var uList: CUList = new CUList(CULs(j).item)
            exCULs.update(j, uList)
            ey_tid.update(j, 0)
            LAU.update(j, X.sumCu + X.sumCru + X.sumNu + X.sumNru)
            CUTIL.update(j, X.sumCu + X.sumCru)
          }
        }
      } else {
        ey_tid.update(j, 0)
        LAU.update(j, X.sumCu + X.sumCru + X.sumNu + X.sumNru)
        CUTIL.update(j, X.sumCu + X.sumCru)
      }
    }
    var HT: HashMap[ArrayBuffer[Int], Int] = HashMap()
    var newT: ArrayBuffer[Int] = null

    for (ex <- X.elements) {
      newT = new ArrayBuffer[Int]()

      for (j <- st + 1 until CULs.size) {
        breakable {
          if (exCULs(j) == null)
            break

          var eylist: List[E_CUL_List] = CULs(j).elements.toList
          while (ey_tid(j) < eylist.size && eylist(ey_tid(j)).tid < ex.tid) {
            ey_tid.update(j, (ey_tid(j) + 1))
          }
          if (ey_tid(j) < eylist.size && eylist(ey_tid(j)).tid == ex.tid) {
            newT.append(j)

          } else {
            LAU.update(j, LAU(j) - ex.Nu - ex.Nru)

            if (LAU(j) < minUtility) {
              exCULs.update(j, null)
              extSz = extSz - 1
              p_laprune += 1
            }

          }
        }
      }
      if (newT.size == extSz) {

        UpdateClosed(X, CULs, st, exCULs, newT, ex, ey_tid, length)

      } else {
        breakable {
          if (newT.size == 0)
            break

          var remainingUtility: Int = 0
          if (merging_flag) {
            if (!HT.contains(newT)) {

              HT.put(newT, exCULs(newT(newT.size - 1)).elements.size)

              for (i <- newT.size - 1 until (-1) by -1) {
                var CULListOfItem: CUList = exCULs(newT(i))
                var Y: E_CUL_List = CULs(newT(i)).elements(ey_tid(newT(i)))

                var element: E_CUL_List = new E_CUL_List(ex.tid, ex.Nu + Y.Nu - ex.Pu, remainingUtility, ex.Nu, 0)
                if (i > 0) {
                  element.Ppos = exCULs(newT(i - 1)).elements.size
                } else {
                  element.Ppos = -1
                }
                CULListOfItem.addElement(element)
                remainingUtility += Y.Nu - ex.Pu
              }
            } else {
              var dupPos: Int = HT(newT)
              UpdateElement(X, CULs, st, exCULs, newT, ex, dupPos, ey_tid)

            }

          } else {

            for (i <- newT.size - 1 until -1 by -1) {
              var CULListOfItem: CUList = exCULs(newT(i))
              var Y: E_CUL_List = CULs(newT(i)).elements(ey_tid(newT(i)))

              var element: E_CUL_List = new E_CUL_List(ex.tid, ex.Nu + Y.Nu - ex.Pu, remainingUtility, ex.Nu, 0)
              if (i > 0) {
                element.Ppos = exCULs(newT(i - 1)).elements.size
              } else {
                element.Ppos = -1
              }
              CULListOfItem.addElement(element)
              remainingUtility += Y.Nu - ex.Pu

            }
          }
        }
      }
      for (j <- st + 1 until CULs.size)
        CUTIL.update(j, CUTIL(j) + ex.Nu + ex.Nru)
    }
    var filter_CULs: ArrayBuffer[CUList] = new ArrayBuffer[CUList]
    for (j <- st + 1 until CULs.size) {
      breakable {
        if (CUTIL(j) < minUtility || exCULs(j) == null) {
          p_cprune += 1
          break

        } else {
          if (length > 1) {
            exCULs(j).sumCu += CULs(j).sumCu + X.sumCu - X.sumCpu
            exCULs(j).sumCru += CULs(j).sumCru
            exCULs(j).sumCpu += X.sumCu
          }
          filter_CULs.append(exCULs(j))
        }
      }
    }

    return filter_CULs
  }
  def UpdateClosed(X: CUList, CULs: ArrayBuffer[CUList], st: Int, exCULs: ArrayBuffer[CUList], newT: ArrayBuffer[Int], ex: E_CUL_List, ey_tid: ArrayBuffer[Int], length: Int) = {
    var nru: Int = 0
    for (j <- newT.size - 1 until (-1) by -1) {
      var ey: CUList = CULs(newT(j))
      var eyy: E_CUL_List = ey.elements(ey_tid(newT(j)))

      exCULs(newT(j)).sumCu += ex.Nu + eyy.Nu - ex.Pu
      exCULs(newT(j)).sumCru += nru
      exCULs(newT(j)).sumCpu += ex.Nu
      nru = nru + eyy.Nu - ex.Pu
    }
  }

  def UpdateElement(X: CUList, CULs: ArrayBuffer[CUList], st: Int, exCULs: ArrayBuffer[CUList], newT: ArrayBuffer[Int], ex: E_CUL_List, dupPos: Int, ey_tid: ArrayBuffer[Int]) = {
    var nru: Int = 0
    var pos: Int = dupPos
    for (j <- newT.size - 1 until (-1) by -1) {

      var ey: CUList = CULs(newT(j))
      var eyy: E_CUL_List = ey.elements(ey_tid(newT(j)))
      exCULs(newT(j)).elements(pos).Nu += ex.Nu + eyy.Nu - ex.Pu
      exCULs(newT(j)).sumNu += ex.Nu + eyy.Nu - ex.Pu
      exCULs(newT(j)).elements(pos).Nru += nru
      exCULs(newT(j)).sumNru += nru
      exCULs(newT(j)).elements(pos).Pu += ex.Nu
      nru = nru + eyy.Nu - ex.Pu
      pos = exCULs(newT(j)).elements(pos).Ppos
    }
  }
}