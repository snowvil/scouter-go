// Seam: AlertCache.scala is the realtime alert ring. AgentManager.alertTooManyChange puts
// MAYBE_OBJ_DUP here directly (not through AlertCore); the oracle records it.
package scouter.server.core.cache

import java.util.ArrayList
import scouter.lang.pack.AlertPack

object AlertCache {
  private val added = new ArrayList[AlertPack]()
  def put(alert: AlertPack): Unit = synchronized { added.add(alert) }
  def take(): ArrayList[AlertPack] = synchronized { val out = new ArrayList[AlertPack](added); added.clear(); out }
}
