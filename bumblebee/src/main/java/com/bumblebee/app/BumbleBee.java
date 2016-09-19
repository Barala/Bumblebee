package com.bumblebee.app;

/**
 * @author ooo
 *
 */
public class BumbleBee 
{
	private static String dirPath = "/home/barala/Desktop/apache-cassandra-2.1.13/data/data/vnodetesting/table1-011b37a57c8e11e6befc6f65bc3254e2";
	private static String hostName = "172.26.147.166";
	
    public static void main( String[] args )
    {
        /**
         * init client info
         * process sstables
         * 	read SSTable
         * 	check token range
         * 	write sstables
         */
    	String[] args1 = {"-d", hostName,dirPath};
    	
    	ClientInfo clientInfo = new ClientInfo(args1);
    	clientInfo.initClientRanges();
    	ProcessSStable processSStable = new ProcessSStable(clientInfo.getClient());
    	processSStable.getClientEndPointRanges();
    	processSStable.initUniqueMap();
        System.out.println( "Main utility" );

    }
}
