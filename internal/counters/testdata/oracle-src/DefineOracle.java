// Test oracle only (not built by Go). Generated outputs live in ../oracle and ../define.
// CountersOracle embeds normalizeCountersXml and the JAXB classes copied verbatim from
// Scouter scouter.server/src/main/java/scouter/server/CounterManager.java at
// dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5 (Copyright 2015 the original author or
// authors, Apache License 2.0). See ../../CONTRACT.md for the javac/java commands.
import org.w3c.dom.*;
import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.nio.file.*;

// args: customFile(or "-" for none) name disp familyName icon subobject(true/false) edit(true/false) out
// Mirrors CounterManager.appendObjectType/editOrAppendObjectType + XmlUtil + normalizeCountersXml.
public class DefineOracle {
  public static void main(String[] a) throws Exception {
    DocumentBuilder b = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document doc;
    if (a[0].equals("-")) doc = b.newDocument(); else { doc = b.parse(new File(a[0])); doc.getDocumentElement().normalize(); }
    Element root = (Element) doc.getElementsByTagName("Counters").item(0);
    Element types = (Element) doc.getElementsByTagName("Types").item(0);
    if (root == null) { root = doc.createElement("Counters"); doc.appendChild(root); }
    if (types == null) { types = doc.createElement("Types"); root.appendChild(types); }
    Element target = null;
    if (a[6].equals("true")) {
      NodeList list = doc.getElementsByTagName("ObjectType");
      for (int i = 0; i < list.getLength(); i++) { Element e = (Element) list.item(i); if (a[1].equals(e.getAttribute("name"))) { target = e; break; } }
    }
    if (target == null) { target = doc.createElement("ObjectType"); types.appendChild(target); }
    target.setAttribute("name", a[1]); target.setAttribute("disp", a[2]); target.setAttribute("family", a[3]);
    target.setAttribute("icon", a[4]); target.setAttribute("sub-object", a[5]);
    TransformerFactory tf = TransformerFactory.newInstance("com.sun.org.apache.xalan.internal.xsltc.trax.TransformerFactoryImpl", DefineOracle.class.getClassLoader());
    Transformer t = tf.newTransformer();
    t.setOutputProperty(OutputKeys.INDENT, "yes");
    t.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
    StringWriter w = new StringWriter();
    t.transform(new DOMSource(doc), new StreamResult(w));
    String out = new CountersOracle().normalizeCountersXml(w.toString());
    Files.write(Paths.get(a[7]), out.getBytes("UTF-8"));
  }
}
