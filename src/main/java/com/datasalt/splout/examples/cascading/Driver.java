package com.datasalt.splout.examples.cascading;

import org.apache.hadoop.util.ProgramDriver;

public class Driver extends ProgramDriver {

	public Driver() throws Throwable {
		addClass("generator", ApacheAccessLogGenerator.class, "Generates a fake access.log");
		addClass("indexer", LogIndexer.class, "Executes the Cascading pipeline and deploys the results to a local Splout SQL.");
	}
	
	public static void main(String args[]) throws Throwable {
		new Driver().driver(args);
	}
}
