// Seam: AlertCore.scala queues the pack for the AlertCore daemon (store, summary, cache, tag count).
// AgentManager's INACTIVE_OBJECT/ACTIVATED_OBJECT go here; the oracle records them.
package scouter.server.core

import java.util.ArrayList
import scouter.lang.pack.AlertPack

object AlertCore {
  private val added = new ArrayList[AlertPack]()
  def add(p: AlertPack): Unit = synchronized { added.add(p) }
  def take(): ArrayList[AlertPack] = synchronized { val out = new ArrayList[AlertPack](added); added.clear(); out }
}
