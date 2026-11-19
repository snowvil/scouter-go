// Seam: GISDataUtil.scala loads a MaxMind legacy LookupService from geoip_data_city_file. The oracle sets
// isOk and the lookup table directly. Like LookupService.getLocation, every call returns a new Location
// (GeoIpUtil mutates loc.city), or null for an address that is not in the table and has no default.
package scouter.server.geoip

import java.net.InetAddress
import java.util.HashMap
import com.maxmind.geoip.Location

object GISDataUtil {
  @volatile var ok = false
  @volatile var lookups = 0
  private val table = new HashMap[String, Array[String]]()
  @volatile var fallback: Array[String] = null

  def clear(): Unit = synchronized { table.clear(); fallback = null }
  def put(ip: String, country: String, city: String): Unit = synchronized { table.put(ip, Array(country, city)) }
  def putMissing(ip: String): Unit = synchronized { table.put(ip, null) }

  def isOk(): Boolean = ok

  def getGeoIPInfos(ip: InetAddress): Location = synchronized {
    lookups += 1
    val key = ip.getHostAddress
    val v = if (table.containsKey(key)) table.get(key) else fallback
    if (v == null) return null
    val loc = new Location()
    loc.countryCode = v(0)
    loc.city = v(1)
    loc
  }
}
