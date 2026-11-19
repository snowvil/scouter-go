// Seam: the original AgentManager object owns ObjectMap plus alert/text/pod/plugin side effects and
// a 1s inactive-check daemon. Only the members the compiled handlers call are kept, with the original
// Scala signatures (java.util.List[Int]); removeAgents/clearInactive bodies mirror AgentManager.scala:219-228,379-386.
package scouter.server.core

import java.util.{ArrayList, Collections, Enumeration, LinkedHashMap, List}
import scouter.lang.pack.{MapPack, ObjectPack}
import scouter.server.db.ObjectWR

object AgentManager {
  private val objMap = new LinkedHashMap[Int, ObjectPack]()

  def seed(p: ObjectPack): Unit = synchronized { objMap.put(p.objHash, p) }
  def reset(): Unit = synchronized { objMap.clear() }
  def snapshotHashes(): List[Int] = synchronized { new ArrayList[Int](objMap.keySet()) }

  def getAgent(objHash: Int): ObjectPack = synchronized { objMap.get(objHash) }
  def getAgent(objName: String): ObjectPack = synchronized {
    val it = objMap.values().iterator()
    while (it.hasNext) { val p = it.next(); if (objName == p.objName) return p }
    null
  }
  def getAgentName(objHash: Int): String = synchronized { val p = objMap.get(objHash); if (p == null) null else p.objName }
  def isActive(objHash: Int): Boolean = synchronized { val p = objMap.get(objHash); p != null && p.alive }
  def getObjPacks(): Enumeration[ObjectPack] = synchronized { Collections.enumeration(new ArrayList[ObjectPack](objMap.values())) }
  private def hashes(f: ObjectPack => Boolean): List[Int] = synchronized {
    val l = new ArrayList[Int](); val it = objMap.values().iterator()
    while (it.hasNext) { val p = it.next(); if (f(p)) l.add(p.objHash) }
    l
  }
  def getLiveObjHashList(): List[Int] = hashes(p => p.alive)
  def getObjHashList(): List[Int] = hashes(p => true)
  def getLiveObjHashList(objType: String): List[Int] = hashes(p => p.alive && objType == p.objType)
  def getObjHashList(objType: String): List[Int] = hashes(p => objType == p.objType)
  def getObjHashListAsString(objType: String): String = {
    val sb = new StringBuilder(); val it = getObjHashList(objType).iterator()
    while (it.hasNext) { if (sb.length > 0) sb.append(','); sb.append(it.next()) }
    sb.toString
  }
  def getObjList(objType: String): List[ObjectPack] = synchronized {
    val l = new ArrayList[ObjectPack](); val it = objMap.values().iterator()
    while (it.hasNext) { val p = it.next(); if (objType == p.objType) l.add(p) }
    l
  }
  def getCurrentObjects(objType: String): MapPack = new MapPack()
  def getDailyObjects(date: String, objType: String): MapPack = new MapPack()
  def getPeriodicObjects(sDate: String, eDate: String, objType: String): MapPack = new MapPack()

  // original AgentManager.scala:219-228: collect alive == false, then objMap.remove(o.objHash)
  def clearInactive(): Unit = synchronized {
    val death = new ArrayList[ObjectPack](); val it = objMap.values().iterator()
    while (it.hasNext) { val p = it.next(); if (!p.alive) death.add(p) }
    val d = death.iterator(); while (d.hasNext) objMap.remove(d.next().objHash)
  }
  // original AgentManager.scala:379-386: objMap.remove(objHash); if (permanent) ObjectWR.remove(objHash)
  def removeAgents(objHashList: List[Int], permanent: Boolean): Unit = synchronized {
    val it = objHashList.iterator()
    while (it.hasNext) { val h = it.next(); objMap.remove(h); if (permanent) ObjectWR.remove(h) }
  }
}
