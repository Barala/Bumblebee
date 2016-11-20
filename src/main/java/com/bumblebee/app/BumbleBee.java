package com.bumblebee.app;

import java.io.IOException;

/**
 * @author ooo
 *
 */
public class BumbleBee 
{
	private static String dirPath = "/home/barala/Desktop/apache-cassandra-2.1.13/data/data/vnodetesting/table1-b72ed441807411e695e66f65bc3254e2";
	private static String hostName = "172.26.147.166";
	private static boolean debugMode = false;
	private static String schemaPath = "/home/barala/Bumblebee/bumblebee/src/test/resources/schema/schema.cql";
	private static String outputDir= "/home/barala/Bumblebee";
	
    public static void main( String[] args )
    {

    	if(debugMode){
    		String[] args1 = {"-d", hostName,dirPath,"-sp",schemaPath,"-op",outputDir};
    		args = args1;
    	}
    	
    	ClientInfo clientInfo = new ClientInfo(args);
    	clientInfo.initClientRanges();

    	try {
        	ProcessSStable processSStable = new ProcessSStable(clientInfo.getClient(),clientInfo.getDifrectory(),clientInfo.getOutputDirPath(),clientInfo.getSchemaPath());
			processSStable.runner();
			System.out.println( "Processed without any glitch!!" );
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
