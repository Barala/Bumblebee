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
