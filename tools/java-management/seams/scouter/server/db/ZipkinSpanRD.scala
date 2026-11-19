// Seam: span file reader. Referenced by XLogService span paths; no oracle case reaches it.
package scouter.server.db

object ZipkinSpanRD {
    def getByGxid(date: String, guid: Long): List[Array[Byte]] = Nil
}
