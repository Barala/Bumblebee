package com.bumblebee.app;

/**
 * 
 * @author ooo
 *
 * * first gets the token range distribution of target cluster
 * * Generates all unique sets which represents set of unique ranges of a ring
 * * read sstables
 * * write back them to sstable 
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
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
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
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.DefaultConnectionFactory;
import org.apache.cassandra.streaming.StreamConnectionFactory;
import org.apache.cassandra.utils.FBUtilities;

public class ProcessSStable {

	private static Map<String,List<Range<Token>>> uniqueRange = new HashMap<>();
	private static Client client;
	private static final boolean debugMode = false;
	private static File directory;
	private static Map<String,List<KeyAttributes>> nodesAndDecoratedKey = new HashMap<>();
	private static Map<String, CFMetaData> allCfMetaData = new HashMap<>();
	private static String timestamp;
	private static String outputDirPath;
	private static String schema;
	
	public ProcessSStable(Client externalClient, File dir, String outoutDirPath, String schemaPath) {
		client = externalClient;
		outputDirPath=outoutDirPath;
		directory = dir;
		schema= schemaPath;
		timestamp= UUID.randomUUID().toString();
	}
	
	
	public void runner() throws IOException{
		uniqueRange = CassandraUtil.generateUniqueRange(client.endpointToRanges);
		initCfMetaData(schema);
		readSStable();
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
	private static void readSStable() throws IOException{
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
		    }else{
		    	throw new IllegalArgumentException("Please provide valid schema");
		    }
		    readSSTable(descriptor, metadata);
		}
	}
	
	  /**
     *
     * @param descthe descriptor of the sstable to read from
     * @param metadata Metadata to read and write back to new sstable
     * @throws IOException on failure to read/write input/output
     */
    private static void readSSTable(Descriptor desc, CFMetaData metadata)
	    throws IOException {
	readSSTable(SSTableReader.open(desc,metadata), metadata, desc);
    }

    static void readSSTable(SSTableReader reader, CFMetaData metadata,Descriptor desc) throws IOException
    {
        SSTableIdentityIterator row;
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
             * open sstable writers corresponding to each unique node
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
                for(String node : nodesAndDecoratedKey.keySet()){
                	if(doesContainDeocratedKey(nodesAndDecoratedKey.get(node), decoratedKey)){
                		SSTableWriter writer = sstableWritersForEachNode.get(node);
                        while(row.hasNext()) {
                            cf.addAtom(row.next());
                        }
                        writer.append(decoratedKey,cf);
                        break;
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
            	writer.closeAndOpenReader().selfRef().release();
            }
            
            
            /**
             * This approach marked all the rows deleted because of timestamp conflict.
             * In cassandra-2.1.13 they don't allow us to expose the cfid but in later version we can.
             * 
             * TODO upgrade this after version change
             */ 
/*            for(Map.Entry<String, List<KeyAttributes>> entry : nodesAndDecoratedKey.entrySet()){
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
            }*/
             
        	nodesAndDecoratedKey.clear();
        }
        finally
        {
            scanner.close();
        }
    }
    
    
    /**
     * Since in future we use KeyAttribute instead of decorated keys. So this is  to check whether decorated key does exist or not
     * 
     * TODO implement equals method for KeyAttributes
     * 
     * @param listOfKeyAttributes
     * @param decoratedKey
     * @return
     */
    private static boolean doesContainDeocratedKey(List<KeyAttributes> listOfKeyAttributes, DecoratedKey decoratedKey){
    	for(KeyAttributes keyAttributes : listOfKeyAttributes){
    		if(keyAttributes.getDecoratedKey().equals(decoratedKey)){
    			return true;
    		}
    	}
    	return false;
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
    	String parentDirPath = outputDirPath+"/data/"+timestamp+"/"+node+"/"+cfName+"/";
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
    	for(Entry<String, List<Range<Token>>> entry : uniqueRange.entrySet()){
    		for(Range<Token> range : entry.getValue()){
    			if(range.contains(token)){
    				return entry.getKey();
    			}
    		}
    	}
    	return null;
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
