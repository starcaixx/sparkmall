package com.work.util

import java.util.{Date, Random}

import scala.collection.mutable.ListBuffer

/**
  * @author star 
  */
object RandomDate {

  def apply(startData:Date,endDate:Date,step:Int): RandomDate = {
    val rd = new RandomDate
    val avgStepTime: Long = (endDate.getTime-startData.getTime)/step

    rd.maxTimeStep = avgStepTime*2
    rd.lastDateTime = startData.getTime
    rd
  }

  class RandomDate{

    var lastDateTime = 0l
    var maxTimeStep = 0l

    def getRandomDate() = {
      val timeStep: Int = new Random().nextInt(maxTimeStep.toInt)
      lastDateTime = lastDateTime + timeStep
      new Date(lastDateTime)
    }
  }
}


