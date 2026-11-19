// Seam: Agent TCP forwarding (AgentCall.scala:40-72) is outside this oracle. Original signatures; any call is a case bug.
package scouter.server.netio

import scouter.io.{DataInputX, DataOutputX}
import scouter.lang.pack.{MapPack, ObjectPack}

object AgentCall {
  def call(o: ObjectPack, cmd: String, param: MapPack): MapPack =
    throw new UnsupportedOperationException("Agent branch outside management oracle: " + cmd)
  def call(o: ObjectPack, cmd: String, param: MapPack, handler: (Int, DataInputX, DataOutputX) => Unit): Unit =
    throw new UnsupportedOperationException("Agent branch outside management oracle: " + cmd)
}
