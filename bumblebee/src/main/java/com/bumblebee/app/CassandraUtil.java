package com.bumblebee.app;

/**
 * @author barala
 */
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
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
}
