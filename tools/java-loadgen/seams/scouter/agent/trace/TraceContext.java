package scouter.agent.trace;

import scouter.lang.pack.XLogDiscardTypes;

/** Seam for the original TraceContext: only the fields DataProxy.sendProfile reads and updates (same names and types). */
public class TraceContext {
    public long txid;
    public long gxid;
    public int profileCount;
    public int profileSize;
    public long startTime;
    public byte xType;
    public XLogDiscardTypes.XLogDiscard discardType;
    public int serviceHash;
}
