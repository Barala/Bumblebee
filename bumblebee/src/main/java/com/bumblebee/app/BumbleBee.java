package com.bumblebee.app;

/**
 * @author ooo
 *
 */
public class BumbleBee 
{
    public static void main( String[] args )
    {
        /**
         * init client info
         * process sstables
         * 	read SSTable
         * 	check token range
         * 	write sstables
         */
    	
    	ClientInfo clientInfo = new ClientInfo(args);
    	clientInfo.initClientRanges();
        System.out.println( "Main utility" );

    }
}
