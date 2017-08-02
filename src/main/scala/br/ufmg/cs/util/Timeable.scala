package br.ufmg.cs.util

trait Timeable extends Logging {
  /** measuring elapsed time */
  def time[R](block: => R)(implicit label: String = null): R = {  
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    val _label = Option(label).getOrElse(Thread.currentThread.getStackTrace()(2))
    logWarning (s"(${_label}): " +
       s" elapsed time: " + ( (t1 - t0) * 10e-6 ) + " ms")
    result
  }
}
