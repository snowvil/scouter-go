// Package-private bridge so the oracle can call TelegrafInputHandler.count with a fixed receivedTime.
package scouter.server.http.handler;
import java.io.IOException;
import scouter.server.http.model.InfluxSingleLine;
public final class TelegrafOracleAccess { public static void count(InfluxSingleLine line, String remoteAddress) throws IOException { TelegrafInputHandler.getInstance().count(line, remoteAddress); } }
