package mapreducejava.fileparser;

import java.io.StringReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class Bz2FileParser{


	private static Pattern namePattern;
	private static Pattern linkPattern;
	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	}
	
	
	public static String format(String line){
		
		StringBuilder outputFormat = new StringBuilder();
		
		try {
			
			// Configure parser.
			SAXParserFactory spf = SAXParserFactory.newInstance();
			
			spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			SAXParser saxParser = spf.newSAXParser();
			XMLReader xmlReader = saxParser.getXMLReader();
			// Parser fills this list with linked page names.
			List<String> linkPageNames = new LinkedList<>();
			xmlReader.setContentHandler(new WikiParser(linkPageNames));

			//BZip2CompressorInputStream inputStream = new BZip2CompressorInputStream(new FileInputStream(inputFile));
			//reader = new BufferedReader(new InputStreamReader(inputStream));
			//String line;
			if (line != null) {
				// Each line formatted as (Wiki-page-name:Wiki-page-html).
				int delimLoc = line.indexOf(':');
				String pageName = line.substring(0, delimLoc);
				String html = line.substring(delimLoc + 1);
				Matcher matcher = namePattern.matcher(pageName);
				if (!matcher.find()) {
					// Skip this html file, name contains (~).
					return "Invalid";
				}

				// Parse page and fill list of linked pages.
				linkPageNames.clear();
				try {
					xmlReader.parse(new InputSource(new StringReader(html)));
				} catch (Exception e) {
					// Discard ill-formatted pages.
					return "Invalid";
				}
				
				//Name of the node
				outputFormat.append(pageName);
				outputFormat.append("##");

				//Number of outlinks from the node
				int noOfOutlinks = linkPageNames.size();

				//Formatting the list of outlinks in a particular pattern
				//nameaOfNode##link1~Link2~...~linkN
				for (String link : linkPageNames){

					if(noOfOutlinks == 1){

						outputFormat.append(link);

					}else{

						outputFormat.append(link);
						outputFormat.append("~");

					}

					noOfOutlinks--;
				}

				//outputFormat.append("##");
				//outputFormat.append("0.0");
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// return the string computed in the format defined above
		return outputFormat.toString();
	}
	
	private static class WikiParser extends DefaultHandler {
		/** List of linked pages; filled by parser. */
		private List<String> linkPageNames;
		/** Nesting depth inside bodyContent div element. */
		private int count = 0;

		public WikiParser(List<String> linkPageNames) {
			super();
			this.linkPageNames = linkPageNames;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			super.startElement(uri, localName, qName, attributes);
			if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
				// Beginning of bodyContent div element.
				count = 1;
			} else if (count > 0 && "a".equalsIgnoreCase(qName)) {
				// Anchor tag inside bodyContent div element.
				count++;
				String link = attributes.getValue("href");
				if (link == null) {
					return;
				}
				try {
					// Decode escaped characters in URL.
					link = URLDecoder.decode(link, "UTF-8");
				} catch (Exception e) {
					// Wiki-weirdness; use link as is.
				}
				// Keep only html filenames ending relative paths and not containing tilde (~).
				Matcher matcher = linkPattern.matcher(link);
				if (matcher.find()) {
					linkPageNames.add(matcher.group(1));
				}
			} else if (count > 0) {
				// Other element inside bodyContent div.
				count++;
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			super.endElement(uri, localName, qName);
			if (count > 0) {
				// End of element inside bodyContent div.
				count--;
			}
		}
	}


}