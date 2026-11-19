// Seam: TextPermWR.scala:71-81 caches and queues the permanent text write. The oracle records the call.
package scouter.server.db

import java.util.ArrayList

object TextPermWR {
  private val added = new ArrayList[Array[AnyRef]]()
  def add(divHash: String, hash: Int, text: String): Unit = synchronized { added.add(Array(divHash, Integer.valueOf(hash), text)) }
  def take(): ArrayList[Array[AnyRef]] = synchronized { val out = new ArrayList[Array[AnyRef]](added); added.clear(); out }
}
