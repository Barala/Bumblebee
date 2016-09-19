package com.bumblebee.app;

/**
 * @author barala
 */
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
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

    public static CFMetaData tableFromCQL(InputStream source, UUID cfid) throws IOException, RequestValidationException {
        String schema = CharStreams.toString(new InputStreamReader(source, "UTF-8"));
        CFStatement statement = (CFStatement) QueryProcessor.parseStatement(schema);
        String keyspace = "";
        try {
            keyspace = statement.keyspace() == null ? "turtles" : statement.keyspace();
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
}
