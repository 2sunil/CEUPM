package CEUPM

import scala.util._
import scala.collection.mutable._

class E_CUL_List {
  var tid: Int = 0
  var Nu: Int = 0
  var Nru: Int = 0
  var Pu: Int = 0
  var Ppos: Int = 0

  def this(tid: Int, nu: Int, nru: Int, pu: Int, ppos: Int) {
    this()
    this.tid = tid
    this.Nu = nu
    this.Nru = nru
    this.Pu = pu
    this.Ppos = ppos
  }
}