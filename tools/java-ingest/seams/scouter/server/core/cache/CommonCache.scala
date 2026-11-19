// Seam: CommonCache.scala (getDailyObjects/getPeriodicObjects caching, not called by the oracle).
package scouter.server.core.cache

object CommonCache {
  def put(key: Any, value: Any, time: Long): Unit = {}
  def get(key: Any): Any = null
}
