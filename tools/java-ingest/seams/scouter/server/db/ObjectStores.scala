// Seams: ObjectWR/ObjectRD are the daily object file writer/reader and AlertWR the alert writer
// (imported by AgentManager). The oracle has no database: writes are dropped, reads are empty
// (AgentManager.read is never called by the original either).
package scouter.server.db

import java.util.ArrayList
import scouter.lang.pack.ObjectPack

object ObjectWR {
  def add(obj: ObjectPack): Unit = {}
  def remove(objHash: Int): Unit = {}
}

object ObjectRD {
  def getObjectList(date: String): java.util.List[ObjectPack] = new ArrayList[ObjectPack]()
}

object AlertWR
