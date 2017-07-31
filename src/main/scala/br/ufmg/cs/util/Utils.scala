package br.ufmg.cs.util

trait Timeable extends Logging {
  /** measuring elapsed time */
  def time[R](block: => R): R = {  
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    logInfo (s"(${Thread.currentThread.getStackTrace()(2)}): " +
       s" elapsed time: " + ( (t1 - t0) * 10e-6 ) + " ms")
    result
  }
}
