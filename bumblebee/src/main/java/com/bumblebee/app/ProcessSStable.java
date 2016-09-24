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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.DefaultConnectionFactory;
import org.apache.cassandra.streaming.StreamConnectionFactory;
import org.apache.cassandra.utils.FBUtilities;

public class ProcessSStable {

	private static Map<String,Set<Range<Token>>> uniqueRange = new HashMap<>();
	private final Client client;
	private static final boolean debugMode = false;
	private static File directory;
	private static Map<String,List<KeyAttributes>> nodesAndDecoratedKey = new HashMap<>();
	private static Map<String, CFMetaData> allCfMetaData = new HashMap<>();
	private static String timestamp;
	private static String OUTPUT_DIR_PATH;
	
	public ProcessSStable(Client client, File dir, String outoutDirPath) {
		this.client = client;
		OUTPUT_DIR_PATH=outoutDirPath;
		directory = dir;
		timestamp= UUID.randomUUID().toString();
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
	 * @param schemaPath
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
//		    	String cql = "CREATE TABLE vnodetesting.table1 ( id int, PRIMARY KEY (id) );";
//		    	metadata = CFMetaData.compile(cql, descriptor.ksname);
		    }else{
		    	throw new IllegalArgumentException("Please provide valid schema");
		    }
		    readSSTable(descriptor, metadata);
		}
	}
	
	  /**
     *
     * @param descthe descriptor of the sstable to read from
     * @param outs PrintStream to write the output to
     * @param excludes keys to exclude from export
     * @param metadata Metadata to print keys in a proper format
     * @throws IOException on failure to read/write input/output
     */
    private static void readSSTable(Descriptor desc, CFMetaData metadata)
	    throws IOException {
	readSSTable(SSTableReader.open(desc,metadata), metadata, desc);
    }

    static void readSSTable(SSTableReader reader, CFMetaData metadata,Descriptor desc) throws IOException
    {
        SSTableIdentityIterator row,rowForDecoratedKey;
        ISSTableScanner scanner = reader.getScanner();
        Map<String, SSTableWriter> sstableWritersForEachNode = new HashMap<>();
        
        try
        {
            while (scanner.hasNext()){
                row = (SSTableIdentityIterator) scanner.next();
                DecoratedKey decoratedKey = row.getKey();
                Token token = decoratedKey.getToken();
                String node =whichNodeItBelongs(token); 
                if(node!=null){
                	if(!nodesAndDecoratedKey.containsKey(node)){
                		nodesAndDecoratedKey.put(node, new ArrayList<>());
                	}
                		nodesAndDecoratedKey.get(node).add(new KeyAttributes(decoratedKey, row.dataSize));
                }else{
                	throw new IllegalArgumentException("Please check your token compare logic @whichNodeItBelongs");
                }
            }
            
            /**
             * TODO now we open sstable writers parallel which can cause high cpu load.
             * IT's better to fetch rows for given decorated key through random access  
             * open sstable writers corresponding to each unique pair
             * it should be Map<Node,SSTableWriter>
             */
            for(String node : nodesAndDecoratedKey.keySet()){
            	String fileName = manipulateModifiedSSTableName(node, metadata.cfName, desc.filenameFor(Component.DATA));
            	SSTableWriter writer = new SSTableWriter(fileName, 
            			nodesAndDecoratedKey.get(node).size(), 
            			ActiveRepairService.UNREPAIRED_SSTABLE, metadata, 
            			StorageService.getPartitioner(), 
            			new MetadataCollector(metadata.comparator));
            	sstableWritersForEachNode.put(node, writer);
            }
            
            scanner = reader.getScanner();
            while(scanner.hasNext()){
            	row = (SSTableIdentityIterator) scanner.next();
            	ColumnFamily cf = row.getColumnFamily();
                DecoratedKey decoratedKey = row.getKey();
                long datasize = row.dataSize;
                KeyAttributes keyAttribute = new KeyAttributes(decoratedKey, datasize);
                for(String node : nodesAndDecoratedKey.keySet()){
                	if(nodesAndDecoratedKey.get(node).contains(keyAttribute)){
                		SSTableWriter writer = sstableWritersForEachNode.get(node);
                        while(row.hasNext()) {
                            cf.addAtom(row.next());
                        }
                        writer.append(decoratedKey,cf);
                	}
                }
            }
            
            /**
             * close all the writers for this sstable
             * 
             * This writer will not build summary part because we are not releasing the reference [FIXME]
             */
            
            for(String node : sstableWritersForEachNode.keySet()){
            	SSTableWriter writer = sstableWritersForEachNode.get(node);
            	writer.close();
            }
            
            
            /**
             * This approach marked all the rows deleted because of timestamp conflict.
             * In cassandra-2.1.13 they don't allow us to expose the cfid but in later version we can.
             * 
             * TODO upgrade this after version change
             * 
             * for(Map.Entry<String, List<KeyAttributes>> entry : nodesAndDecoratedKey.entrySet()){
            	String nodes = entry.getKey();
            	String fileName = manipulateModifiedSSTableName(nodes, metadata.cfName, desc.filenameFor(Component.DATA));
                SSTableWriter writer = new SSTableWriter(fileName, entry.getValue().size(), ActiveRepairService.UNREPAIRED_SSTABLE, metadata, StorageService.getPartitioner(), new MetadataCollector(metadata.comparator));
            	RandomAccessReader dfile = reader.openDataReader();
            	for(KeyAttributes keyAttributes : entry.getValue()){
            		DecoratedKey decoratedKey = keyAttributes.getDecoratedKey();
            		long dataSize = keyAttributes.getDataSize();
            		RowIndexEntry indexEntry = reader.getPosition(decoratedKey, SSTableReader.Operator.EQ);
            		if(indexEntry==null){
            		    continue;
            		}
            		dfile.seek(indexEntry.position);
            		//ColumnFamily.setMetaData(metadata);
            		rowForDecoratedKey = new SSTableIdentityIterator(reader, dfile, decoratedKey,dataSize);
            		ColumnFamily cf =  rowForDecoratedKey.getColumnFamily();
                    while(rowForDecoratedKey.hasNext()) {
                        cf.addAtom(rowForDecoratedKey.next());
                    }
                    writer.append(decoratedKey,cf);
            	}
            	writer.closeAndOpenReader().selfRef().release();
            }
             */
        	nodesAndDecoratedKey.clear();
        }
        finally
        {
            scanner.close();
        }
    }
    
    /**
     * appends timestamp to avoid the over riding cases
     * 
     * @param node
     * @param cfName
     * @param sstable
     * @param path
     * @return
     */
    private static String manipulateModifiedSSTableName(String node, String cfName, String sstable){
    	String[] sstablePath = sstable.split("/");
    	String SSTableDataPart = sstablePath[sstablePath.length-1];
    	String parentDirPath = OUTPUT_DIR_PATH+"/data/"+timestamp+"/"+node+"/"+cfName+"/";
    	createDir(parentDirPath);
    	return (parentDirPath+SSTableDataPart);
    }
    
    
    private static void createDir(String path){
    	Path filePath = Paths.get(path);
    	if(!Files.exists(filePath)){
    		try{
    			Files.createDirectories(filePath);
    		}catch(IOException e){
    			e.printStackTrace();
    		}
    	}
    }
    
	/**
	 * 
	 * @param token
	 * @return
	 */
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
		// remove "/" part from node ip address
		if(node_one.contains("/")){
			node_one=node_one.replace("/", "");
		}
		if(node_two.contains("/")){
			node_two=node_two.replace("/", "");
		}
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
