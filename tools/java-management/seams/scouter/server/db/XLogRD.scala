// Seam: XLog file reader (XLogRD.scala). Serves synthetic XLog bytes from memory in time order instead of
// XLogDataReader/IndexTimeFile. The try/catch around each scan is the original's (XLogRD.scala:41-48,
// 61-68,82-88): it also swallows a handler's Scala non-local return (NonLocalReturnControl is a
// Throwable, not an Exception), which is how XLogService stops TRANX_LOAD_TIME_GROUP at max and
// SEARCH_XLOG_LIST at req_search_xlog_max_count. The time range is inclusive like IndexTimeFile.read.
package scouter.server.db

import java.util.Vector

object XLogRD {
    private class Rec(val date: String, val time: Long, val txid: Long, val xlog: Array[Byte])
    private val records = new java.util.ArrayList[Rec]()

    def seamClear(): Unit = records.synchronized { records.clear() }
    def seamAdd(date: String, time: Long, txid: Long, xlog: Array[Byte]): Unit = records.synchronized { records.add(new Rec(date, time, txid, xlog)) }

    private def select(date: String, fromTime: Long, toTime: Long): java.util.List[Rec] = records.synchronized {
        val out = new java.util.ArrayList[Rec]()
        val it = records.iterator()
        while (it.hasNext) { val r = it.next(); if (r.date == date && r.time >= fromTime && r.time <= toTime) out.add(r) }
        java.util.Collections.sort(out, new java.util.Comparator[Rec] { def compare(a: Rec, b: Rec) = java.lang.Long.compare(a.time, b.time) })
        out
    }

    def readByTimeLimitCount(date: String, fromTime: Long, toTime: Long, lastBucketTime: Long, limitCount: Int, handler: (Long, Array[Byte]) => Int) {
        try {
            val it = select(date, if (lastBucketTime > 0) lastBucketTime else fromTime, toTime).iterator()
            while (it.hasNext) { val r = it.next(); handler(r.time, r.xlog) }
        } catch {
            case e: Exception => e.printStackTrace()
            case _: Throwable =>
        }
    }

    def readByTime(date: String, fromTime: Long, toTime: Long, handler: (Long, Array[Byte]) => Any) {
        try {
            val it = select(date, fromTime, toTime).iterator()
            while (it.hasNext) { val r = it.next(); handler(r.time, r.xlog) }
        } catch {
            case e: Exception => e.printStackTrace()
            case _: Throwable =>
        }
    }

    def readFromEndTime(date: String, fromTime: Long, toTime: Long, handler: (Long, Array[Byte]) => Any) {
        try {
            val l = select(date, fromTime, toTime)
            java.util.Collections.reverse(l)
            val it = l.iterator()
            while (it.hasNext) { val r = it.next(); handler(r.time, r.xlog) }
        } catch {
            case e: Throwable =>
        }
    }

    def getByTxid(date: String, txid: Long): Array[Byte] = records.synchronized {
        val it = records.iterator()
        var found: Array[Byte] = null
        while (found == null && it.hasNext) { val r = it.next(); if (r.date == date && r.txid == txid) found = r.xlog }
        found
    }

    def getByGxid(date: String, guid: Long): Vector[Array[Byte]] = new Vector[Array[Byte]]()
}
