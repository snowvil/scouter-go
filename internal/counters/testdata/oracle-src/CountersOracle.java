// Test oracle only (not built by Go). Generated outputs live in ../oracle and ../define.
// CountersOracle embeds normalizeCountersXml and the JAXB classes copied verbatim from
// Scouter scouter.server/src/main/java/scouter/server/CounterManager.java at
// dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5 (Copyright 2015 the original author or
// authors, Apache License 2.0). See ../../CONTRACT.md for the javac/java commands.
import scouter.lang.counters.CounterEngine;
import javax.xml.bind.*;
import javax.xml.bind.annotation.*;
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

// Oracle: for each input file, CounterEngine.parse result and the verbatim
// CounterManager.normalizeCountersXml output (JAXB) or exception class.
public class CountersOracle {
  public static void main(String[] args) throws Exception {
    CountersOracle o = new CountersOracle();
    for (String a : args) {
      byte[] raw = Files.readAllBytes(Paths.get(a));
      PrintStream old = System.err;
      System.setErr(new PrintStream(new ByteArrayOutputStream()));
      boolean parsed = new CounterEngine().parse(raw);
      System.setErr(old);
      Files.write(Paths.get(a + ".parse"), String.valueOf(parsed).getBytes("UTF-8"));
      String out;
      try { out = o.normalizeCountersXml(new String(raw, "UTF-8")); }
      catch (JAXBException e) { out = "JAXBException"; }
      catch (Throwable t) { out = "EXC:" + t.getClass().getName(); }
      Files.write(Paths.get(a + ".norm"), out.getBytes("UTF-8"));
    }
  }
public String normalizeCountersXml(String xmlString) throws JAXBException {

	JAXBContext jaxbContext = JAXBContext.newInstance(Counters0.class);
	Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
	Counters0 counters = (Counters0) unmarshaller.unmarshal(new StringReader(xmlString));

	if (counters.types == null) {
		counters.types = new Types0(new ArrayList<>());
	}
	List<ObjectType0> objectTypes =
			counters.types.objectType.stream()
					.collect(Collectors.toMap(v -> v.name, v -> v, (x, y) -> y)).values().stream().collect(Collectors.toList());

	if (counters.familys == null) {
		counters.familys = new Familys0(new ArrayList<>());
	}
	if (counters.familys.family == null) {
		counters.familys.family = new ArrayList<>();
	}
	List<Family0> families =
			counters.familys.family.stream()
					.filter(f -> !f.name.contains("zws-metric."))
					.filter(f -> !f.name.contains("zps-metric."))
					.collect(Collectors.toMap(v -> v.name, v -> v, (x, y) -> y)).values().stream().collect(Collectors.toList());

	Counters0 newCounters = new Counters0(new Types0(objectTypes), new Familys0(families));

	Marshaller mar= jaxbContext.createMarshaller();
	mar.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
	StringWriter stringWriter = new StringWriter();
	mar.marshal(newCounters, stringWriter);

	return stringWriter.toString();
}
@XmlRootElement(name = "Counters")
public static class Counters0 {
	@XmlElement(name = "Types")
	Types0 types;
	@XmlElement(name = "Familys")
	Familys0 familys;

	public Counters0() {
	}

	public Counters0(Types0 types, Familys0 familys) {
		this.types = types;
		this.familys = familys;
	}
}

@XmlAccessorType(XmlAccessType.FIELD)
public static class Types0 {
	@XmlElement(name = "ObjectType")
	List<ObjectType0> objectType;

	public Types0() {
	}

	public Types0(List<ObjectType0> objectType) {
		this.objectType = objectType;
	}
}

@XmlAccessorType(XmlAccessType.FIELD)
//@XmlType(propOrder = { "disp", "family", "icon", "name", "sub_object" })
public static class ObjectType0 {
	@XmlAttribute
	public String disp = "";
	@XmlAttribute
	public String family = "";
	@XmlAttribute
	public String icon = "";
	@XmlAttribute
	public String name = "";
	@XmlAttribute(name = "sub-object")
	public String sub_object = "";
}

@XmlAccessorType(XmlAccessType.FIELD)
public static class Familys0 {
	@XmlElement(name = "Family")
	public List<Family0> family;

	public Familys0() {
	}

	public Familys0(List<Family0> family) {
		this.family = family;
	}
}

@XmlAccessorType(XmlAccessType.FIELD)
//@XmlType(propOrder = { "master", "name" })
public static class Family0 {
	@XmlAttribute
	public String master = "";
	@XmlAttribute
	public String name = "";
	@XmlElement(name = "Counter")
	public List<Counter0> counter;
}

@XmlAccessorType(XmlAccessType.FIELD)
//@XmlType(propOrder = { "all", "disp", "icon", "name", "total", "unit" })
public static class Counter0 {
	@XmlAttribute
	public String all = "true";
	@XmlAttribute
	public String disp = "";
	@XmlAttribute
	public String icon = "";
	@XmlAttribute
	public String name = "";
	@XmlAttribute
	public String total = "";
	@XmlAttribute
	public String unit = "";
}
}
