package com.vijet.mr.preprocessing;

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

/**
 * Mapper that preprocess the data and emits each of the outlinks
 */
public class PageRankPreprocessorMapper extends Mapper<Object, Text, Text, Text>{
	private static Pattern namePattern;
	private static Pattern linkPattern;
	private static XMLReader xmlReader;
	private static List<String> linkPageNames;		
	@Override
	protected void setup(
			Mapper<Object, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~|\\?]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");

		// Configure parser.
		SAXParserFactory spf = SAXParserFactory.newInstance();
		try {
			spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			SAXParser saxParser = spf.newSAXParser();
			xmlReader = saxParser.getXMLReader();
			// Parser fills this list with linked page names.
			linkPageNames = new LinkedList<>();
			xmlReader.setContentHandler(new WikiParser(linkPageNames));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringBuilder line = new StringBuilder(value.toString());
		int delimLoc = line.toString().indexOf(':');
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

		if(linkPageNames.isEmpty()){
			context.write(new Text(pageName), new Text(""));
			return;
		}
		for(String link: linkPageNames){
			context.write(new Text(pageName), new Text(link));
			context.write(new Text(link), new Text(""));
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
