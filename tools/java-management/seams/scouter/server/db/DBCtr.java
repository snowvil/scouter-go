// Seam: the original DBCtr object also starts DB writer/purge lifecycle. Only getRootPath is kept,
// with the same body as the original line (DBCtr.scala:30: Configure.getInstance().db_dir).
package scouter.server.db;
public final class DBCtr { public static String getRootPath() { return scouter.server.Configure.getInstance().db_dir; } }
