// Seam: AgentCall.scala:40-72 writes to the Agent TCP session. Original signature; the call is recorded
// (objHash, command, encoded MapPack) and answers null like an Agent without a TCP session.
package scouter.server.netio

import scouter.lang.pack.{MapPack, ObjectPack}

object AgentCall {
  def call(o: ObjectPack, cmd: String, param: MapPack): MapPack = {
    AgentCallLog.record(o, cmd, param)
    null
  }
}
