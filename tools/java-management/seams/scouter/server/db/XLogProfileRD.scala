// Seam: profile file reader. Referenced by XLogService TRANX_PROFILE*; no oracle case reaches it.
package scouter.server.db

object XLogProfileRD {
    def getProfile(date: String, txid: Long, max: Int): Array[Byte] = null
    def getFullProfile(date: String, txid: Long, max: Int, handler: (Array[Byte]) => Any) {}
}
