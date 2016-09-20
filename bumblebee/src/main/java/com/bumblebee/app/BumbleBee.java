package com.bumblebee.app;

import java.io.IOException;

/**
 * @author ooo
 *
 */
public class BumbleBee 
{
	private static String dirPath = "/home/barala/Desktop/apache-cassandra-2.1.13/data/data/vnodetesting/table1-011b37a57c8e11e6befc6f65bc3254e2/";
	private static String hostName = "172.26.147.166";
	private static boolean debugMode = true;
	private static String schemaPath = "/home/barala/Bumblebee/bumblebee/src/test/resources/schema/schema.cql";
	
    public static void main( String[] args )
    {
        /**
         * init client info
         * process sstables
         * 	read SSTable
         * 	check token range
         * 	write sstables
         */
    	if(debugMode){
    		String[] args1 = {"-d", hostName,dirPath};
    		args = args1;
    	}
    	
    	
    	ClientInfo clientInfo = new ClientInfo(args);
    	clientInfo.initClientRanges();
    	ProcessSStable processSStable = new ProcessSStable(clientInfo.getClient(),clientInfo.getDifrectory());
    	processSStable.getClientEndPointRanges();
    	processSStable.initUniqueMap();
    	ProcessSStable.initCfMetaData(schemaPath);
    	try {
			ProcessSStable.readSStable();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println( "Main utility" );
    }
}
