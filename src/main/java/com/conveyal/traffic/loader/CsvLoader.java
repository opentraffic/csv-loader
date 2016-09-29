package com.conveyal.traffic.loader;

import com.fasterxml.jackson.databind.util.ISO8601DateFormat;
import io.opentraffic.engine.data.pbf.ExchangeFormat;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.*;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class CsvLoader {

    public static void main(String[] args) {

        try {
            Options options = new Options();

            options.addOption("f", true,
                    "specify CSV file (can be compressed as .csv.gz or csv.zip)");

            options.addOption("u", true,
                    "specify URL for traffic engine file (defaults to 'http://localhost:9000/')");

            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            String urlStr = "http://localhost:4567/locationUpdate";

            if (cmd.hasOption("u"))
                urlStr = cmd.getOptionValue("u");

            if (!cmd.hasOption("f")) {
                System.out.println("CSV input file not specified");
                return;
            }

            System.out.println(cmd.getOptionValue("f"));

            File csvFile = new File(cmd.getOptionValue("f"));

            if (!csvFile.exists()) {
                System.out.println("CSV input file does not exist: " + csvFile.getAbsolutePath());
                return;
            } else {
                long sourceId = UUID.randomUUID().getLeastSignificantBits();
                processCsv(sourceId, csvFile, urlStr);
            }
        } catch (Exception e) {
            e.printStackTrace();
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


            int count = 0;

            Set<String> uniqueVehicles = new HashSet<>();
            Set<Long> uniqueVehIds = new HashSet<>();

            List<ExchangeFormat.VehicleMessage> messages = new ArrayList<ExchangeFormat.VehicleMessage>();

            // this is streaming; a call to getRecords() would read the file into memory
            for (CSVRecord csvRecord : parser) {
                String vehicleIdStr = null;
                String timeStr;
                String lonStr;
                String latStr;

                double lat;
                double lon;
                try{
                    timeStr = csvRecord.get(0);
                    vehicleIdStr = csvRecord.get(1);
                    lonStr = csvRecord.get(3);
                    latStr = csvRecord.get(2);
                    if (csvRecord.size() > 9) {
                        lonStr = csvRecord.get(10);
                        latStr = csvRecord.get(9);
                    }

                    lat = Double.parseDouble(latStr);
                    lon = Double.parseDouble(lonStr);
                }catch(Exception e){
                    e.printStackTrace();
                    System.out.println("failed to read record with values : " + csvRecord.toString());
                    continue;
                }

                long time;
                try {
                    time = parseTimeStrToMilli( timeStr );
                } catch (ParseException ex) {
                    System.out.println("Unable to parse taxi time " + timeStr);
                    continue;
                }

                long vehicleId;
                vehicleId = new BigInteger(vehicleIdStr).longValue();
                uniqueVehicles.add(vehicleIdStr);
                uniqueVehIds.add(vehicleId);

                ExchangeFormat.VehicleMessage vehicleMessage = ExchangeFormat.VehicleMessage.newBuilder()
                        .setVehicleId(vehicleId)
                        .addLocations(ExchangeFormat.VehicleLocation.newBuilder()
                                .setLat(lat)
                                .setLon(lon)
                                .setTimestamp(time))
                        .build();

                messages.add(vehicleMessage);

                if(messages.size() > 10000) {
                    count += messages.size();
                    sendData(url, sourceId, messages);
                    messages.clear();

                    System.out.println(String.format("%.2fM records loaded, %d unique vehicles (%d unique ids)", count / 1e6, uniqueVehicles.size(), uniqueVehIds.size()));
                }
            }

            sendData(url, sourceId, messages);

            parser.close();
            decoder.close();

            if(fileStream != null)
                fileStream.close();

            if(zf != null)
                zf.close();

            if(gzipStream != null)
                gzipStream.close();


        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Unable to parse CSV " + csvFile);
            if(parser != null)
                try {
                    parser.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
        }
    }

    public static void sendData(String url, long sourceId, List<ExchangeFormat.VehicleMessage> messages) {

        boolean dataSent = false;

        while(!dataSent) {
            try {
                CloseableHttpClient client = HttpClients.custom()
                        .setConnectionManager(new PoolingHttpClientConnectionManager())
                        .build();

                ExchangeFormat.VehicleMessageEnvelope vehicleMessageEnvelope = ExchangeFormat.VehicleMessageEnvelope.newBuilder()
                        .setSourceId(sourceId)
                        .addAllMessages(messages)
                        .build();

                byte[] postData = vehicleMessageEnvelope.toByteArray();
                HttpPost httpPost = new HttpPost(url);
                ByteArrayEntity entity = new ByteArrayEntity(postData);
                httpPost.setEntity(entity);
                CloseableHttpResponse res = client.execute(httpPost);

                if (res.getStatusLine().getStatusCode() != 200)
                    System.out.println("not ok: " + res.getStatusLine().getStatusCode() + " " + res.getStatusLine().getReasonPhrase());

                res.close();

                httpPost.releaseConnection();

                dataSent = true;

            } catch (IOException e) {
                System.out.println("Unable to send data to " + url + " retrying in 5s.");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {

                }
            }
        }
    }

    private static long parseTimeStrToMilli(String timeStr) throws ParseException {
        StringBuilder sb = new StringBuilder(timeStr);
        int snipStart = sb.indexOf(".");
        int snipEnd = sb.indexOf("+");

        if (snipEnd == -1)
            snipEnd = sb.indexOf("Z");

        if (snipEnd == -1)
            snipEnd = sb.length();

        String microsString="0.0";
        if (snipStart != -1) {
            microsString = "0"+sb.substring(snipStart,snipEnd);
            sb.delete(snipStart,snipEnd);
            timeStr = sb.toString();
        }
        SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssX");
        SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        ISO8601DateFormat formatter3 = new ISO8601DateFormat();
        Date dt = null;
        try{
            dt = formatter1.parse(timeStr);
        }catch (Exception e){
            try{
                dt = formatter2.parse(timeStr);
            }catch (Exception e2){
                dt = formatter3.parse(timeStr);
            }
        }

        long timeMillis = dt.getTime();
        long millis = (long) (Double.parseDouble(microsString)*1000);

        long time = timeMillis + millis;
        return time;
    }
}
