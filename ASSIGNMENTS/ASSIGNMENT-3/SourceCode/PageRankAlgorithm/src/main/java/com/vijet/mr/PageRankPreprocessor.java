package com.vijet.mr;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Class that preprocess the raw data into the format:
 * LinkId outlink1~outlink2~~pagerankValue (if node is a not a dangling node)
 * LinkId NA~pagerankValue (if node is a dangling node)
 */
public class PageRankPreprocessor {
	//Counter constant
	public enum COUNTERS{
		TOTAL_NUMBER_OF_DOCUMENTS;
	}
	/**
	 * Mapper class that preprocess the data. It reads one line at a time and ignores it if
	 * it contains "~", otherwise, it will build the adjacency list in the following format.
	 * LinkId	outlink1~outlink2~~PagerankValue (For links having outlinks)
	 * LinkId	null~~PagerankValue (For dangling nodes)
	 */
	public static class PageRankPreprocessorMapper extends Mapper<Object, Text, NullWritable, Text>{
		private static Pattern namePattern;
		private static Pattern linkPattern;
		private static XMLReader xmlReader;
		private static List<String> linkPageNames;
		// Stores the number of links
		private static Set<String> linkSet;
		
		@Override
		protected void setup(
				Mapper<Object, Text, NullWritable, Text>.Context context)
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
				linkSet = new HashSet<String>();
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

			linkSet.add(pageName);
			// format the string
			StringBuilder dataString = new StringBuilder();
			for(String link: linkPageNames){
				dataString.append(link+"~");
			}
			if(linkPageNames.isEmpty()){
				dataString.append("NA~");
			}
			dataString.append("~"+"0.0");
			context.write(NullWritable.get(), new Text(pageName+"|"+dataString.toString()));
		}
		
		@Override
		protected void cleanup(Mapper<Object, Text, NullWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// flush the total number of links to the counters.
			context.getCounter(COUNTERS.TOTAL_NUMBER_OF_DOCUMENTS).increment(linkSet.size());
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
	/**
	 * Driver method for this Program
	 */
	public static long preprocess(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "PageRankPreprocessingJob");
		job.setJarByClass(PageRankPreprocessor.class);
		
		job.setMapperClass(PageRankPreprocessorMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		for(int i = 0 ; i <= args.length-2;i++){
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		FileOutputFormat.setOutputPath(job,new Path(args[args.length-1]+"-0"));
		
		job.setNumReduceTasks(0);
		job.waitForCompletion(true);
		
		// get the counters and Return
		long value = job.getCounters().findCounter(COUNTERS.TOTAL_NUMBER_OF_DOCUMENTS).getValue();
		System.out.println("Total no of documents: " + value);
		
		return value;
	}
}
