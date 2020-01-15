package com.cmdi.mro_ftpdata_to_hive.util

import java.util.concurrent.{Executor, Executors}

import scala.collection.mutable

object Test1 {


  def main(args: Array[String]): Unit = {
    var nameMold: mutable.HashMap[String, String] = new mutable.HashMap[String, String]
    nameMold("a") =  "b"
    nameMold("1") =  "c"
    nameMold("2") =  "e"
    nameMold("3") =  "b"
    nameMold += ("5"->"c")
    nameMold.foreach(println(_))
    println(testDef2)
    println(this.getClass.getName)
    println(nameMold.getOrElse("e",null))
    println(nameMold("e"))
    /*val pool = Executors.newFixedThreadPool(10)
    pool.execute(new Runnable {
      override def run(): Unit = {
        Thread.sleep(10)
        println(nameMold("a"))
      }
    })
    pool.execute(new Runnable {
      override def run(): Unit = {
        Thread.sleep(10)
        println(nameMold("a"))
      }
    })
    pool.execute(new Runnable {
      override def run(): Unit = {
        Thread.sleep(10)
        println(nameMold("a"))
      }
    })
    pool.execute(new Runnable {
      override def run(): Unit = {
        Thread.sleep(10)
        println(nameMold("a"))
      }
    })
    pool.execute(new Runnable {
      override def run(): Unit = {
        Thread.sleep(10)
        println(nameMold("a"))
      }
    })
    pool.execute(new Runnable {
      override def run(): Unit = {
        Thread.sleep(10)
        println(nameMold("a"))
      }
    })
    pool.execute(new Runnable {
      override def run(): Unit = {
        Thread.sleep(10)
        println(nameMold("a"))
      }
    })
    pool.execute(new Runnable {
      override def run(): Unit = {
        Thread.sleep(10)
        println(nameMold("a"))
      }
    })
    pool.execute(new Runnable {
      override def run(): Unit = {
        Thread.sleep(10)
        println(nameMold("a"))
      }
    })
    pool.shutdown()*/
    /*println(nameMold.getOrElse("c",-123))
    println(nameMold("c"))*/
  //  var nameMold1: mutable.LinkedHashMap[String, String] = null
     // println(nameMold1(null))
  /*  val buffer =  Array[Array[String]](Array("zzz","bbb"),Array("ccc","ddd"))
    Seq(1).map(x=>{
      buffer
    }).flatMap(x=>{
      //println(x)
      x
    }).foreach(x=>{println(x.toBuffer)})*/
  }
  def testDef2:Int={
  if(1 == 1){
  return 2
  }
    0
  }
}
