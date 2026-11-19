// Java oracle for internal/account XML output. The DOM steps are copied from
// the pinned Scouter source dee47fb5 (AccountFileHandler.scala,
// GroupFileHandler.scala, XmlUtil.scala); only the I/O around them differs.
//
// Usage: java XmlOracle <start.xml> <ops.txt>
// Each ops line is tab separated; every field after the op name is hex UTF-8
// ("-" is Java null). Each op parses the current text, applies the DOM
// change, runs the Xalan transform with indent 2 and prints "OK <hex>" or
// "ERR <class>"; the next op parses the previous output (Go persists it).
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XmlOracle {
	static String writeXml(Document doc) throws Exception {
		TransformerFactory f = TransformerFactory.newInstance("com.sun.org.apache.xalan.internal.xsltc.trax.TransformerFactoryImpl", XmlOracle.class.getClassLoader());
		Transformer t = f.newTransformer();
		t.setOutputProperty(OutputKeys.INDENT, "yes");
		t.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", String.valueOf(2));
		StringWriter w = new StringWriter();
		t.transform(new DOMSource(doc), new StreamResult(w));
		return w.toString();
	}

	static Document parse(byte[] data) throws Exception {
		DocumentBuilder b = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document doc = b.parse(new ByteArrayInputStream(data));
		doc.getDocumentElement().normalize();
		return doc;
	}

	static String field(String hex) {
		if (hex.equals("-")) {
			return null;
		}
		byte[] b = new byte[hex.length() / 2];
		for (int i = 0; i < b.length; i++) {
			b[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
		}
		return new String(b, StandardCharsets.UTF_8);
	}

	static String hex(byte[] b) {
		StringBuilder s = new StringBuilder();
		for (byte x : b) {
			s.append(String.format("%02x", x & 0xff));
		}
		return s.toString();
	}

	// AccountFileHandler.addAccount
	static void addAccount(Document doc, String id, String pass, String group, String email) {
		Node accounts = doc.getElementsByTagName("Accounts").item(0);
		Element accountEle = doc.createElement("Account");
		accountEle.setAttribute("id", id);
		accountEle.setAttribute("pass", pass);
		accountEle.setAttribute("group", group);
		Element emailEle = doc.createElement("Email");
		emailEle.setTextContent(email);
		accountEle.appendChild(emailEle);
		accounts.appendChild(accountEle);
	}

	// AccountFileHandler.editAccount
	static void editAccount(Document doc, String id, String pass, String group, String email) throws Exception {
		NodeList nodeList = doc.getElementsByTagName("Account");
		for (int i = 0; i < nodeList.getLength(); i++) {
			Node node = nodeList.item(i);
			if (node.getNodeType() == Node.ELEMENT_NODE) {
				Element element = (Element) node;
				if (id.equals(element.getAttribute("id"))) {
					element.setAttribute("pass", pass);
					element.setAttribute("group", group);
					element.getElementsByTagName("Email").item(0).setTextContent(email);
					return;
				}
			}
		}
		throw new Exception("Cannot find account id : " + id);
	}

	// GroupFileHandler.setTextValue
	static void setTextValue(Document doc, Element element, String tagName, String text) {
		Element allowElement;
		NodeList nodeList = element.getElementsByTagName(tagName);
		if (nodeList == null || nodeList.getLength() < 1) {
			allowElement = doc.createElement(tagName);
			element.appendChild(allowElement);
		} else {
			allowElement = (Element) nodeList.item(0);
		}
		allowElement.setTextContent(text);
	}

	// GroupFileHandler.addAccountGroup with the policy keys already in
	// MapValue (Hashtable) enumeration order.
	static void addGroup(Document doc, String name, List<String> f) {
		Element groupElement = doc.createElement("Group");
		groupElement.setAttribute("name", name);
		doc.getElementsByTagName("Groups").item(0).appendChild(groupElement);
		Element policyElement = doc.createElement("Policy");
		groupElement.appendChild(policyElement);
		for (int i = 0; i + 1 < f.size(); i += 2) {
			setTextValue(doc, policyElement, f.get(i), String.valueOf(Boolean.parseBoolean(f.get(i + 1))));
		}
	}

	// GroupFileHandler.editGroupPolicy/findAndSet: fields are repeated
	// (group, count, name, text...) in MapPack key order with each group's
	// policy keys already in Hashtable enumeration order.
	static void editPolicy(Document doc, List<String> f) {
		NodeList nodeList = doc.getElementsByTagName("Group");
		int i = 0;
		while (i < f.size()) {
			String group = f.get(i);
			int n = Integer.parseInt(f.get(i + 1));
			i += 2;
			for (int g = 0; g < nodeList.getLength(); g++) {
				Node node = nodeList.item(g);
				if (node.getNodeType() == Node.ELEMENT_NODE) {
					Element element = (Element) node;
					if (group.equals(element.getAttribute("name"))) {
						Element policyElement = (Element) element.getElementsByTagName("Policy").item(0);
						for (int k = 0; k < n; k++) {
							setTextValue(doc, policyElement, f.get(i + 2 * k), f.get(i + 2 * k + 1));
						}
						break;
					}
				}
			}
			i += 2 * n;
		}
	}

	public static void main(String[] args) throws Exception {
		byte[] current = Files.readAllBytes(new File(args[0]).toPath());
		List<String> lines = Files.readAllLines(new File(args[1]).toPath(), StandardCharsets.UTF_8);
		for (String line : lines) {
			if (line.isEmpty()) {
				continue;
			}
			String[] parts = line.split("\t", -1);
			List<String> f = new java.util.ArrayList<String>();
			for (int i = 1; i < parts.length; i++) {
				f.add(field(parts[i]));
			}
			try {
				Document doc = parse(current);
				switch (parts[0]) {
				case "identity":
					break;
				case "addAccount":
					addAccount(doc, f.get(0), f.get(1), f.get(2), f.get(3));
					break;
				case "editAccount":
					editAccount(doc, f.get(0), f.get(1), f.get(2), f.get(3));
					break;
				case "addGroup":
					addGroup(doc, f.get(0), f.subList(1, f.size()));
					break;
				case "editPolicy":
					editPolicy(doc, f);
					break;
				default:
					throw new IllegalArgumentException(parts[0]);
				}
				byte[] out = writeXml(doc).getBytes(StandardCharsets.UTF_8);
				System.out.println("OK " + hex(out));
				current = out;
			} catch (Exception e) {
				System.out.println("ERR " + e.getClass().getSimpleName());
			}
		}
	}
}
