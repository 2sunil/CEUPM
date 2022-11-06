package CEUPM

import scala.util._
import scala.collection.mutable._

class CUList {
  var item: Int = 0

  var sumNu: Int = 0
  var sumNru: Int = 0
  var sumCu: Int = 0
  var sumCru: Int = 0
  var sumCpu: Int = 0

  var elements: ArrayBuffer[(E_CUL_List)] = ArrayBuffer()

  def this(itemIn1: Int) {
    this()
    item = itemIn1
  }

  def addElement(element: E_CUL_List) {
    elements.append((element))

    sumNu += element.Nu
    sumNru += element.Nru
  }
}