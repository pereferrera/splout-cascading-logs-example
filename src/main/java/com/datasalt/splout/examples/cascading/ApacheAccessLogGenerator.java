package com.datasalt.splout.examples.cascading;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ApacheAccessLogGenerator {

	public final static int MILLIS_PER_DAY = 1000 * 60 * 60 * 24;
	public final static String NAV = "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:16.0) Gecko/20100101 Firefox/16.0";
	
	public static void generate(String out, int nUsers, int nPagesPerUser, int nDays, int nPagesInSystem) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(out)));
		for(int user = 0; user < nUsers; user++) {
			String ip = randomIp();
			for(int day = 0; day < nDays; day++) {
				for(int pageView = 0; pageView < nPagesPerUser; pageView++) {
					String date = "[" + date(System.currentTimeMillis() - MILLIS_PER_DAY * day - randomNumber(MILLIS_PER_DAY)) + " +0200]";
					String request = "GET /page" + randomNumber(nPagesInSystem) + ".html HTTP/1.1\" 200 600 \"-\" \"" + NAV + "\"";
					writer.write(ip + " - user" + user + " " + date + " \"" + request + "\n");
				}
			}
		}
		writer.close();
	}
	
	public static String date(long instant) {
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
		return sdf.format(new Date(instant));
	}
	
	public static String randomIp() {
		return randomNumber(200) + "." + randomNumber(200) + "." + randomNumber(200) + "." + randomNumber(200);
	}
	
	public static int randomNumber(int max) {
		return (int)(Math.random() * max);
	}
	
	public static void main(String[] args) throws IOException {
		ApacheAccessLogGenerator.generate("access.log", 100, 15, 10, 100);
	}
}
