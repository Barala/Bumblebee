package com.bumblebee.app;

/**
 * 
 * @author ooo
 *
 */

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.streaming.DefaultConnectionFactory;
import org.apache.cassandra.streaming.StreamConnectionFactory;
import org.apache.cassandra.utils.FBUtilities;

public class ProcessSStable {

	private static Map<String,Set<Range<Token>>> uniqueRange = new HashMap<>();
	private final Client client;
	private static final boolean debugMode = false;
	private static File directory;
	private static Map<String,List<DecoratedKey>> nodesAndDecoratedKey = new HashMap<>();
	private static Map<String, CFMetaData> allCfMetaData = new HashMap<>();
	
	public ProcessSStable(Client client, File dir) {
		this.client = client;
		directory = dir;
	}
	
	
	/**
	 * 
	 * @return
	 */
	public Map<InetAddress, Collection<Range<Token>>> getClientEndPointRanges(){
		return client.getEndpointToRangesMap();
	}
	
	/**
	 * to initiliaze the schema 
	 * @param schemaPaht
	 */
	public static void initCfMetaData(String schemaPath){
		List<String> lines;
		try {
			lines = Files.readAllLines(Paths.get(schemaPath), StandardCharsets.UTF_8);
			for(String line : lines){
				allCfMetaData.put(CassandraUtil.tableFromCQL(line).cfName, CassandraUtil.tableFromCQL(line));
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * @throws IOException 
	 * 
	 */
	public static void readSStable() throws IOException{
		List<String> allSSTableFiles = new ArrayList<>();
		allSSTableFiles = CassandraUtil.generateAbsolutePathOfAllSSTables(directory.getAbsolutePath());
		Config.setClientMode(true);
		Config conf = new Config();
		conf.file_cache_size_in_mb=512;
		try {
//			DatabaseDescriptor.loadConfig().
			DatabaseDescriptor.loadConfig();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//DatabaseDescriptor.loadSchemas(false);
		for(String SSTable : allSSTableFiles){
			Descriptor descriptor = Descriptor.fromFilename(SSTable);
		    // keyspace validation
		    if (Schema.instance.getKSMetaData(descriptor.ksname) == null) {
			System.err.println(String.format("Filename %s references to nonexistent keyspace: %s!", SSTable,
				descriptor.ksname));
			System.exit(1);
		    }
		    CFMetaData metadata;
		    if(allCfMetaData.containsKey(descriptor.cfname)){
		    	metadata = allCfMetaData.get(descriptor.cfname);
		    }else{
		    	throw new IllegalArgumentException("Please provide valid schema");
		    }
		    readSSTable(descriptor, metadata, "");
		}
	}
	
	  /**
     * Export an SSTable and write the resulting JSON to a PrintStream.
     *
     * @param descthe descriptor of the sstable to read from
     * @param outs PrintStream to write the output to
     * @param excludes keys to exclude from export
     * @param metadata Metadata to print keys in a proper format
     * @throws IOException on failure to read/write input/output
     */
    private static void readSSTable(Descriptor desc, CFMetaData metadata, String modifiedSSTablePath)
	    throws IOException {
	readSSTable(SSTableReader.open(desc,metadata), metadata, modifiedSSTablePath);
    }

    static void readSSTable(SSTableReader reader, CFMetaData metadata, String modifiedSSTablePath) throws IOException
    {
        SSTableIdentityIterator row;
        ISSTableScanner scanner = reader.getScanner();
        try
        {
            while (scanner.hasNext())
            {	Map<String, List<DecoratedKey>> nodesAndDecoratedKey1 = new HashMap<>(); 
                row = (SSTableIdentityIterator) scanner.next();
                DecoratedKey decoratedKey = row.getKey();
                Token token = decoratedKey.getToken();
                String node =whichNodeItBelongs(token); 
                if(node!=null){
                	if(nodesAndDecoratedKey1.containsKey(node)){
                		List<DecoratedKey> temp = new ArrayList<>(); 
                				temp = nodesAndDecoratedKey1.get(node);
                		temp.add(decoratedKey);
                		nodesAndDecoratedKey1.put(node, temp);
                	}else{
                		nodesAndDecoratedKey1.put(node, Arrays.asList(decoratedKey));
                	}
                }else{
                	throw new IllegalArgumentException("Please check your token compare logic @whichNodeItBelongs");
                }
            }
            System.out.println("yeah@@");
        }
        finally
        {
            scanner.close();
        }
    }
	
    private static String whichNodeItBelongs(Token token){
    	for(Map.Entry<String, Set<Range<Token>>> entry : uniqueRange.entrySet()){
    		for(Range<Token> range : entry.getValue()){
    			if(range.contains(token)){
    				return entry.getKey();
    			}
    		}
    	}
    	return null;
    }
	/**
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
	 */
	//TODO write it as recursive function only works for <= 3 nodes // inside map use key as Ring and then map it to nodes
	// Move it to UtilPart
	public void initUniqueMap(){
		Map<InetAddress, Collection<Range<Token>>> endPointRanges = getClientEndPointRanges();
		for(Map.Entry<InetAddress, Collection<Range<Token>>> entry : getClientEndPointRanges().entrySet()){
			String entryInetAddress = entry.getKey().toString();
			for(Map.Entry<InetAddress, Collection<Range<Token>>> innerEntry : endPointRanges.entrySet()){
				String innerEntryInetAddress = innerEntry.getKey().toString();
				if(!entryInetAddress.equals(innerEntryInetAddress)){
					Set<Range<Token>> entrySet = new HashSet<>(entry.getValue());
					Set<Range<Token>> innerEntrySet = new HashSet<>(innerEntry.getValue());
					entrySet.retainAll(innerEntrySet);
					if(!uniqueRange.containsKey(generateUniqueName(entryInetAddress, innerEntryInetAddress))){
						if(entrySet!=null && !entrySet.isEmpty()){
							uniqueRange.put(generateUniqueName(entryInetAddress, innerEntryInetAddress),entrySet);
						}
					}
				}
			}
		}
		
		// this is to check  whether node itself have some unique range or not
			Map<String, Set<Range<Token>>> uniqueSingleRange = new HashMap<>(); 
			
			for(Map.Entry<InetAddress, Collection<Range<Token>>> entry : endPointRanges.entrySet()){
				String entryKey = entry.getKey().toString();
				Set<Range<Token>> entrySet = new HashSet<>(entry.getValue());
				for(Map.Entry<String, Set<Range<Token>>> innerEntry : uniqueRange.entrySet()){
					String innerEntryKey = innerEntry.getKey().toString();
					if(innerEntryKey.contains(entryKey)){
						entrySet.removeAll(innerEntry.getValue());
					}
				}
				if(entrySet!=null && !entrySet.isEmpty()){
					uniqueSingleRange.put(entryKey, entrySet);
				}
			}
		
			// this is to combine single and common unique ranges
			for(Map.Entry<String, Set<Range<Token>>> entry : uniqueSingleRange.entrySet()){
				uniqueRange.put(entry.getKey(), entry.getValue());
			}
		
			// handle RF = N case. In this case your all node will share same data 
			if(checkForAllRF(uniqueRange)){
				Set<Range<Token>> onlyRangeForAll = uniqueRange.get(uniqueRange.keySet().toArray()[0]);
				uniqueRange.clear();
				uniqueRange.put("AllNode", onlyRangeForAll);
			}
			
			if(debugMode){
				System.out.println("-------------------Expected------------------");
				int countActualTotal=0;
				for(Map.Entry<InetAddress, Collection<Range<Token>>> entry : endPointRanges.entrySet()){
					int entryTotal = entry.getValue().size();
					System.out.println(entry.getKey() +" :size: "+ entryTotal);
					countActualTotal+=entryTotal;
				}
				
				System.out.println("actual total ranges :: " + countActualTotal);
				
				System.out.println("******************Calculated*****************");
				
				int countTotal=0;
				System.out.println(uniqueRange.keySet());
				for(Map.Entry<String, Set<Range<Token>>> entry : uniqueRange.entrySet()){
					int totalRangesForEntry = entry.getValue().size();
					System.out.println(entry.getKey() +" :: size :: "+ totalRangesForEntry);
					countTotal+=totalRangesForEntry;
				}
				System.out.println("total Keys :: " + countTotal);
			}
	}
	
	/**
	 * I think no need to use this after changing the initUniqueMap logic
	 * 
	 * @param uniqueRange
	 * @return
	 */
	//TODO remove it after this fix
	private static boolean checkForAllRF(Map<String,Set<Range<Token>>> uniqueRange){
		boolean flag=false;
		if(uniqueRange.size()>1){
			Set<Range<Token>> checkForThis = uniqueRange.get(uniqueRange.keySet().toArray()[0]);
			for(Map.Entry<String, Set<Range<Token>>> entry : uniqueRange.entrySet()){
				if(entry.getValue().containsAll(checkForThis) && checkForThis.containsAll(entry.getValue())){
					flag=true;
				}else{
					return false;
				}
			}
		}
		return flag;
	}

	/**
	 * 
	 * @param node_one
	 * @param node_two
	 * @return
	 */
	private static String generateUniqueName(String node_one, String node_two){
		if(node_one.compareTo(node_two)>0){
			return node_one+"_"+node_two;
		}
		return node_two+"_"+node_one;			
	}

	
	public File getDir() {
		return directory;
	}


	public static abstract class Client
    {
        private final Map<InetAddress, Collection<Range<Token>>> endpointToRanges = new HashMap<>();
        private IPartitioner partitioner;

        /**
         * Initialize the client.
         * Perform any step necessary so that after the call to the this
         * method:
         *   * partitioner is initialized
         *   * getEndpointToRangesMap() returns a correct map
         * This method is guaranteed to be called before any other method of a
         * client.
         */
        public abstract void init(String keyspace);

        /**
         * Stop the client.
         */
        public void stop() {}

        /**
         * Provides connection factory.
         * By default, it uses DefaultConnectionFactory.
         *
         * @return StreamConnectionFactory to use
         */
        public StreamConnectionFactory getConnectionFactory()
        {
            return new DefaultConnectionFactory();
        }

        /**
         * Validate that {@code keyspace} is an existing keyspace and {@code
         * cfName} one of its existing column family.
         */
        public abstract CFMetaData getCFMetaData(String keyspace, String cfName);

        public Map<InetAddress, Collection<Range<Token>>> getEndpointToRangesMap()
        {
            return endpointToRanges;
        }

        protected void setPartitioner(String partclass) throws ConfigurationException
        {
            setPartitioner(FBUtilities.newPartitioner(partclass));
        }

        protected void setPartitioner(IPartitioner partitioner)
        {
            this.partitioner = partitioner;
            // the following is still necessary since Range/Token reference partitioner through StorageService.getPartitioner
            DatabaseDescriptor.setPartitioner(partitioner);
        }

        public IPartitioner getPartitioner()
        {
            return partitioner;
        }

        protected void addRangeForEndpoint(Range<Token> range, InetAddress endpoint)
        {
            Collection<Range<Token>> ranges = endpointToRanges.get(endpoint);
            if (ranges == null)
            {
                ranges = new HashSet<>();
                endpointToRanges.put(endpoint, ranges);
            }
            ranges.add(range);
        }
    }
}
