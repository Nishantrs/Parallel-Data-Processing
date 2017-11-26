package pagerankmatrix.assignment5.preprocessing;


import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;


// Code snippet used from Example code
public class MapperForReading extends Mapper<Object, Text, Text, Text>{

	private static Pattern namePattern;
	private static Pattern linkPattern;
	static {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	}

	private SAXParserFactory spf;
	private SAXParser saxParser;
	private XMLReader xmlReader;
	private List<String> linkPageNames;
	



	public void setup(Context ctx) {
		// Configure parser.
		spf = SAXParserFactory.newInstance();
		try {
			spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			saxParser = spf.newSAXParser();
			xmlReader = saxParser.getXMLReader();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Parser fills this list with linked page names.
		linkPageNames = new LinkedList<>();
		xmlReader.setContentHandler(new WikiParser(linkPageNames));
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString().trim();

		// Each line formatted as (Wiki-page-name:Wiki-page-html).
		int delimLoc = line.indexOf(':');
		String pageName = line.substring(0, delimLoc);
		String html = line.substring(delimLoc + 1);
		Matcher matcher = namePattern.matcher(pageName);
		if (!matcher.find()) {
			// Skip this html file, name contains (~).
			return;
		}

		// Parse page and fill list of linked pages.
		linkPageNames.clear();
		try {
			xmlReader.parse(new InputSource(new StringReader(html)));
		} catch (Exception e) {
			// Discard ill-formatted pages.
			return;
		}

		// If page has no outlinks then emit empty string as value
		if(linkPageNames.size() == 0 && !pageName.isEmpty()){
			
			context.write(new Text(pageName),new Text(""));
			
		}else{
			
			// Emit page along with its outlinks and outlinks with empty string as value
			// In this way, we can get dangling nodes and nodes with outlinks in one map reduce job
			for (String link : linkPageNames){

				if(!link.isEmpty() && !pageName.isEmpty()){
					context.write(new Text(pageName),new Text(link));
					context.write(new Text(link),new Text(""));
				}
			}

		}

	}



	/** Parses a Wikipage, finding links inside bodyContent div element. */
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

