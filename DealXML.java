package chaohBIM;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


public class DealXML {
	
	public static String ReaddomXMl(String fileName,String attri) {
        try {
            DocumentBuilder domBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            InputStream input = new FileInputStream(fileName);
            Document doc = domBuilder.parse(input);
            Element root = doc.getDocumentElement();
            NodeList para = root.getChildNodes();
            if (para != null) {
                for (int i = 0, size = para.getLength(); i < size; i++) {
                    Node student = para.item(i);
                    for (Node node = student.getFirstChild(); node != null; node = node.getNextSibling()) {
                        if (node.getNodeType() == Node.ELEMENT_NODE) {
                            if (node.getNodeName().equals(attri)) {
                                return node.getFirstChild().getNodeValue();                               
                            }
                        }
                    }

                }
            }
            else{
            	return "hello";
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
		return attri;
    }

  
  //search all the nodes
  private static NodeList selectNodes(String expression, Object source) {
	    try {
	      return (NodeList) XPathFactory.newInstance().newXPath().evaluate(expression, source, XPathConstants.NODESET);
	    } catch (XPathExpressionException e) {
	      e.printStackTrace();
	      return null;
	    }
	  }
 
  // output file to disk
  public static void output(Node node, String filename) {
	    TransformerFactory transFactory = TransformerFactory.newInstance();
	    try {
	      Transformer transformer = transFactory.newTransformer();
	      // 设置各种输出属性
	      transformer.setOutputProperty("encoding", "UTF-8");
	      transformer.setOutputProperty("indent", "no");
	      DOMSource source = new DOMSource();
	      // 将待转换输出节点赋值给DOM源模型的持有者(holder)
	      source.setNode(node);
	      StreamResult result = new StreamResult();
	      if (filename == null) {
	        // 设置标准输出流为transformer的底层输出目标
	        result.setOutputStream(System.out);
	      } else {
	        result.setOutputStream(new FileOutputStream(filename));
	      }
	      // 执行转换从源模型到控制台输出流
	      transformer.transform(source, result);
	    } catch (TransformerConfigurationException e) {
	      e.printStackTrace();
	    } catch (TransformerException e) {
	      e.printStackTrace();
	    } catch (FileNotFoundException e) {
	      e.printStackTrace();
	    }
	  }
  
  //three input part write function
  /*
   * input 1: xml file function
   * input 2: which child part
   * input 3: input string
   * */
  	public static void writefile(String filename,int index,String val){
  		try{
  		  Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(filename);
  		  Element root = document.getDocumentElement();
  	
  		  String expression = "/docs/pp";
  		  NodeList nodelist = selectNodes(expression, root);
  		  NodeList childNodeList = nodelist.item(0).getChildNodes();
  		  
  		  childNodeList.item(index).setTextContent(val);
  		  output(root, filename);
  	  }
  	  catch (SAXException e) {
  		  e.printStackTrace();
  		} catch (IOException e) {
  		  e.printStackTrace();
  		} catch (ParserConfigurationException e) {
  		  e.printStackTrace();
  		}
  	}
  	
  	// four input write function
  	/*
  	 * input 1: xml filename
  	 * input 2: which part
  	 * input 3: which child part
  	 * input 4: input string
  	 * */
	public static void writefile(String filename,int part, int index, String val){
  		try{
  		  Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(filename);
  		  Element root = document.getDocumentElement();
  	
  		  String expression = "/docs/p"+part;
  		  NodeList nodelist = selectNodes(expression, root);
  		  /*
  		  *	0: tilte
  		  *	1: detail in the passage
  		  *	2: doc filename
  		  */
  		  NodeList childNodeList = nodelist.item(0).getChildNodes();
  		  
  		  childNodeList.item(index).setTextContent(val);
  		  output(root, filename);
  	  }
  	  catch (SAXException e) {
  		  e.printStackTrace();
  		  e.printStackTrace();
  		  e.printStackTrace();
  		  e.printStackTrace();
  		} catch (IOException e) {
  		  e.printStackTrace();
  		} catch (ParserConfigurationException e) {
  		  e.printStackTrace();
  		}
  	}
	
	// four input write function
		//different from the above with part 2
	  	/*
	  	 * input 1: xml filename
	  	 * input 2: which part
	  	 * input 3: which child part
	  	 * input 4: input string
	  	 * */
		public static void writefile(String filename,String part, int index, String val){
	  		try{
	  		  Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(filename);
	  		  Element root = document.getDocumentElement();
	  	
	  		  String expression = "/docs/p"+part;
	  		  NodeList nodelist = selectNodes(expression, root);
	  		  /*
	  		  *	0: tilte
	  		  *	1: detail in the passage
	  		  *	2: doc filename
	  		  */
	  		  NodeList childNodeList = nodelist.item(0).getChildNodes();
	  		  
	  		  childNodeList.item(index).setTextContent(val);
	  		  output(root, filename);
	  	  }
	  	  catch (SAXException e) {
	  		  e.printStackTrace();
	  		  e.printStackTrace();
	  		  e.printStackTrace();
	  		  e.printStackTrace();
	  		} catch (IOException e) {
	  		  e.printStackTrace();
	  		} catch (ParserConfigurationException e) {
	  		  e.printStackTrace();
	  		}
	  	}
}