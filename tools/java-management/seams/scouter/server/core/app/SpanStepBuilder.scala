// Seam: zipkin span -> step conversion (SpanStepBuilder.scala). Referenced by XLogService's span paths
// only; no oracle case reaches it.
package scouter.server.core.app

import scouter.lang.pack.SpanPack
import scouter.lang.step.StepSingle
import scala.collection.mutable.ListBuffer

object SpanStepBuilder {
    def toSteps(gxid: Long, txid: Long, packList: ListBuffer[SpanPack]): (ListBuffer[StepSingle], SpanPack) = (new ListBuffer[StepSingle], null)
}
