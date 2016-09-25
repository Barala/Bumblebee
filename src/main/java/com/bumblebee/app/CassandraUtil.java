package com.bumblebee.app;

/**
 * @author barala
 */
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.locator.SimpleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.CharStreams;


public class CassandraUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(CassandraUtil.class);
	
	public static CFMetaData tableFromCQL(InputStream source) throws IOException, RequestValidationException {
        return tableFromCQL(source, null);
    }

	/**
	 * 
	 * @param source
	 * @param cfid
	 * @return
	 * @throws IOException
	 * @throws RequestValidationException
	 */
    public static CFMetaData tableFromCQL(InputStream source, UUID cfid) throws IOException, RequestValidationException {
        String schema = CharStreams.toString(new InputStreamReader(source, "UTF-8"));
        CFStatement statement = (CFStatement) QueryProcessor.parseStatement(schema);
        String keyspace = "";
        try {
            keyspace = statement.keyspace() == null ? "jarvis" : statement.keyspace();
        } catch (AssertionError e) { // if -ea added we should provide lots of warnings that things probably wont work
            logger.warn("Remove '-ea' JVM option when using sstable-tools library");
            keyspace = "jarvis";
        }
        statement.prepareKeyspace(keyspace);
        if(Schema.instance.getKSMetaData(keyspace) == null) {
        	Schema.instance.setKeyspaceDefinition(KSMetaData.newKeyspace(keyspace, SimpleStrategy.class, new HashMap<String,String>(), true,Collections.<CFMetaData>emptyList()));
        }
        CFMetaData cfm;
        cfm = ((CreateTableStatement) statement.prepare().statement).getCFMetaData();
        return cfm;
    }
    
    /**
     * 
     * @param schema
     * @return
     * @throws IOException
     * @throws RequestValidationException
     */
    public static CFMetaData tableFromCQL(String schema) throws IOException, RequestValidationException {
        CFStatement statement = (CFStatement) QueryProcessor.parseStatement(schema);
        String keyspace = "";
        try {
            keyspace = statement.keyspace() == null ? "jarvis" : statement.keyspace();
        } catch (AssertionError e) { // if -ea added we should provide lots of warnings that things probably wont work
            logger.warn("Remove '-ea' JVM option when using sstable-tools library");
            keyspace = "jarvis";
        }
        statement.prepareKeyspace(keyspace);
        if(Schema.instance.getKSMetaData(keyspace) == null) {
        	Schema.instance.setKeyspaceDefinition(KSMetaData.newKeyspace(keyspace, SimpleStrategy.class, new HashMap<String,String>(), true,Collections.<CFMetaData>emptyList()));
        }
        CFMetaData cfm;
        cfm = ((CreateTableStatement) statement.prepare().statement).getCFMetaData();
        return cfm;
    }
    
    
    /**
     * To generate absolute path of all the sstables for given directory
     *
     * @param String path of dirtectory
     * @return list of abssolute path of all the sstables
     */
    public static List<String> generateAbsolutePathOfAllSSTables(String directoryPath) {
	List<String> absolutePathOfAllSSTables = new ArrayList<String>();
	File dir = new File(directoryPath);
	if (dir.isDirectory()) {
	    File[] directoryListing = dir.listFiles();
	    for (File file : directoryListing) {
		if (file.getName().endsWith("Data.db")) {
		    absolutePathOfAllSSTables.add(file.getAbsolutePath());
		}
	    }
	    return absolutePathOfAllSSTables;
	}
	absolutePathOfAllSSTables.add(directoryPath);
	return absolutePathOfAllSSTables;
    }
    

    /**
     * 
     * @param endPointRanges
     * @return
     * 
     * 
     * 
	 * Initialize the uniqueRangeMap
	 * 
	 * Purpose :: We will be having ranges for each node address and they will also have some common ranges (aka overlapping)
	 * 
	 *   * Example :: There is one cluster with three nodes {N1,N2,N3} along with RF=2
	 *   * N1 -> r1, r2, r3, r4, r5, r10
	 *   * N2 -> r7, r8, r9, r10, r1,r2
	 *   * N3 -> r7, r8, r9, r3, r4, r5
	 * 
	 *   * So we need to extract unique ranges
	 *   * N12 -> r1,r2,r10
	 *   * N13 -> r3,r4,r5
	 *   * N23 -> r7,r8,r9
	 *   
	 *   * Soln ::
	 *   key   Value
	 *   ..........
	 *   r1 -> N1,N2
	 *   r2 -> N1,N2
	 *   r3 -> N1,N3
	 *   r4 -> N1,N3
	 *   r5 -> N1,N3
	 *   r7 -> N2,N3
	 *   r8 -> N2,N3
	 *   r9 -> N2,N3
	 *   r10-> N1,N2
	 *   
	 *   Now do It reverse
	 *   key		Value
	 *   ................
	 *   N2_N1 -> r1,r2,r10
	 *   N3_N1 -> r3,r4,r5
	 *   N3_N2 -> r7,r8,r9
	 *   
	 *   Suggested by Lakshay  
     * 
     * 
     */
    public static Map<String,List<Range<Token>>> generateUniqueRange(Map<InetAddress,Collection<Range<Token>>> endPointRanges){
    	Map<Range<Token>,List<String>> rangeMap = new HashMap<>(); 
    	for(Map.Entry<InetAddress, Collection<Range<Token>>> entry : endPointRanges.entrySet()){
    		InetAddress inetAddress = entry.getKey();
    		String nodeIp = inetAddress.toString().replace("/", "");
    		for(Range<Token> range : entry.getValue()){
    			if(!rangeMap.containsKey(range)){
    				rangeMap.put(range, new ArrayList<String>());
    			}
    			rangeMap.get(range).add(nodeIp);
    		}
    	}
    	Map<String,List<Range<Token>>> uniqueRanges = new HashMap<>();
    	for(Map.Entry<Range<Token>, List<String>> entry : rangeMap.entrySet()){
    		Range<Token> range = entry.getKey();
    		String fullCombinedName = getCombinedFolderName(entry.getValue());
    		if(!uniqueRanges.containsKey(fullCombinedName)){
    			uniqueRanges.put(fullCombinedName, new ArrayList<Range<Token>>());
    		}
    		uniqueRanges.get(fullCombinedName).add(range);
    	}
    	
    	return uniqueRanges;
    }
    
    /**
     * for given list returns combined name
     * 
     * @param allNodes
     * @return combinedName
     */
    private static String getCombinedFolderName(List<String> allNodes){
    	Collections.sort(allNodes);
    	String fullCombinedName = "";
    	for(String node : allNodes){
    		if(fullCombinedName.isEmpty()){
    			fullCombinedName = node;
    		}else{
    			fullCombinedName = fullCombinedName + "_" +node;
    		}
    		
    	}
    	return fullCombinedName;
    }
}
