package com.conveyal.traffic.loader;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import com.conveyal.traffic.data.ExchangeFormat;

public class CsvLoader {
	
	public static void main(String[] args) throws org.apache.commons.cli.ParseException, IOException {
		
		Options options = new Options();
		
		options.addOption("f", true, "specify CSV file (can be compressed as .csv.gz or csv.zip)");
		
		options.addOption("u", true, "specify URL for traffic engine file (defauls to 'http://localhost/')");
		
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);
		
		String urlStr = "http://localhost/";
		
		if(cmd.hasOption("u"))
			urlStr = cmd.getOptionValue("u");
		
		if(!cmd.hasOption("f")) {
			System.out.println("CSV input file not specified");
			return;
		}
		
		System.out.println(cmd.getOptionValue("f"));
		
		File csvFile = new File(cmd.getOptionValue("f"));
		
		if(!csvFile.exists()) {
			System.out.println("CSV input file does not exist: " + csvFile.getAbsolutePath());
			return;
		}
		else {
			long sourceId = UUID.randomUUID().getLeastSignificantBits();
			processCsv(sourceId, csvFile, urlStr);
		}
			
	}

	public static void processCsv(long sourceId, File csvFile, String url) {

		// create a semi-random source id for this csv load attempt
		// prevents vehicle id collisions
		
		
		CSVParser parser = null;
		try {
			
			InputStream fileStream = null;
			ZipFile zf = null;
			InputStream gzipStream = null;
			Reader decoder = null;
			
			if(csvFile.getName().toLowerCase().endsWith(".zip")) {
				zf = new ZipFile(csvFile);
				Enumeration e = zf.entries();
				ZipEntry entry = (ZipEntry) e.nextElement();
				decoder = new InputStreamReader(zf.getInputStream(entry), Charset.forName("UTF-8"));
			}
			else if(csvFile.getName().toLowerCase().endsWith(".gz")) {
				fileStream = new FileInputStream(csvFile);
				gzipStream = new GZIPInputStream(fileStream);
				decoder = new InputStreamReader(gzipStream, Charset.forName("UTF-8"));
			} 
			else {
				fileStream = new FileInputStream(csvFile);
				decoder = new InputStreamReader(fileStream, Charset.forName("UTF-8"));
			}
				 
			
			parser = new CSVParser(decoder, CSVFormat.RFC4180);
			for (CSVRecord csvRecord : parser.getRecords()) {

				String timeStr = csvRecord.get(0);
				String vehicleIdStr = csvRecord.get(1);
				String lonStr = csvRecord.get(2);
				String latStr = csvRecord.get(3);
				
				double lat;
				double lon;
				
				lat = Double.parseDouble(latStr);
				lon = Double.parseDouble(lonStr);
				
				long time;
				try {
					time = parseTimeStrToMicros( timeStr );
				} catch (ParseException ex) {
					System.out.println("Unable to parse taxi time " + timeStr);
					continue;
				}
				
				long vehicleId;
				vehicleId = Long.parseLong(vehicleIdStr);
				
				ExchangeFormat.VehicleMessage vehicleMessage = ExchangeFormat.VehicleMessage.newBuilder()
				 .setSourceId(sourceId)
				 .setVehicleId(vehicleId)
				 .addLocations(ExchangeFormat.VehicleLocation.newBuilder()
						 .setLat(lat)
						 .setLon(lon)
						 .setTimestamp(time))
				 .build();
				
				byte[] postData = vehicleMessage.toByteArray();
				HttpPost httpPost = new HttpPost(url);
				ByteArrayEntity entity = new ByteArrayEntity(postData);
				httpPost.setEntity(entity);
				HttpClient client = new DefaultHttpClient();
				client.execute(httpPost);
				
			}
			
			parser.close();
			decoder.close();
			
			if(fileStream != null)
				fileStream.close();
			
			if(zf != null)
				zf.close();
			
			if(gzipStream != null)
				gzipStream.close();

			
		} catch (IOException e) {
			System.out.println("Unable to parse CSV " + csvFile);
			if(parser != null)
				try {
					parser.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
		} 
	}

	private static long parseTimeStrToMicros(String timeStr) throws ParseException {
		StringBuilder sb = new StringBuilder(timeStr);
		int snipStart = sb.indexOf(".");
		int snipEnd = sb.indexOf("+");
		String microsString="0.0";
		if (snipStart != -1) {
			microsString = "0"+sb.substring(snipStart,snipEnd);
			sb.delete(snipStart,snipEnd);
			timeStr = sb.toString();
		}
			
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssX");
		
		Date dt = formatter.parse(timeStr);
		long timeMillis = dt.getTime();
		long micros = (long) (Double.parseDouble(microsString)*1000000);
		
		long time = timeMillis*1000 + micros;
		return time;
	}
}
