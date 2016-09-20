package com.bumblebee.app;

/**
 * 
 * @author ooo
 *
 */

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.streaming.DefaultConnectionFactory;
import org.apache.cassandra.streaming.StreamConnectionFactory;
import org.apache.cassandra.utils.FBUtilities;

public class ProcessSStable {

	private static Map<String,Set<Range<Token>>> uniqueRange = new HashMap<>();
	private final Client client;
	/**
	 * 
	 * access here token range stuff
	 *
	 */
	
	/**
	 * 
	 * read SSTables
	 * 
	 * for each
	 * 	store all the keys in map<Node,SSTable>
	 * 	open writer and write it to SSTables
	 *	
	 */
	public ProcessSStable(Client client) {
		this.client = client;
	}
	
	/**
	 * 
	 * @return
	 */
	public Map<InetAddress, Collection<Range<Token>>> getClientEndPointRanges(){
		return client.getEndpointToRangesMap();
	}
	
	/**
	 * Initialize the uniqueRangeMap
	 */
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
					if(uniqueRange.containsKey(generateUniqueName(entryInetAddress,innerEntryInetAddress)) 
							|| uniqueRange.containsKey(generateUniqueName(innerEntryInetAddress, entryInetAddress))){
						String key = generateUniqueName(entryInetAddress, innerEntryInetAddress);
						Set<Range<Token>> alreadyExist = uniqueRange.get(key);
						if(alreadyExist==null){
							key = generateUniqueName(innerEntryInetAddress, entryInetAddress);
							alreadyExist = uniqueRange.get(key);
						}
						alreadyExist.addAll(entrySet);
						uniqueRange.put(key,alreadyExist);
					}else{
						uniqueRange.put(generateUniqueName(entryInetAddress, innerEntryInetAddress),entrySet);
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
					// in this case get unique range
					// also check does big key contain the samller one or not
					String innerEntryKey = innerEntry.getKey().toString();
					if(innerEntryKey.contains(entryKey)){
						entrySet.removeAll(innerEntry.getValue());
					}
				}
				if(entrySet!=null){
					uniqueSingleRange.put(entryKey, entrySet);
				}
			}
		
			// this is to combine single and common unique ranges
			for(Map.Entry<String, Set<Range<Token>>> entry : uniqueSingleRange.entrySet()){
				uniqueRange.put(entry.getKey(), entry.getValue());
			}
			
		System.out.println(uniqueRange.keySet());
	}

	/**
	 * 
	 * @param node_one
	 * @param node_two
	 * @return
	 */
	private static String generateUniqueName(String node_one, String node_two){
		return node_one+"_"+node_two;
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
