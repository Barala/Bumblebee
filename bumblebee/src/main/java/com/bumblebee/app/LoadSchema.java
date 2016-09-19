package com.bumblebee.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.exceptions.SyntaxException;

/**
 * this class will be used to load the schema
 * 
 * @author ooo
 *
 */
public class LoadSchema {
	private static String schemaFilePath;
	private static final String CANNOT_FIND_FILE="";
	private static final String IMPORTED_SCHEMA="";
	private static final String FAILED_TO_IMPORT_SCHEMA="";
	
	public LoadSchema(String schemaPath) {
		schemaFilePath = schemaPath;
	}
	
	public static void loadSchema() throws IOException{
		File schemaFile = new File(schemaFilePath);
        if (!schemaFile.exists()) {
            System.err.printf(CANNOT_FIND_FILE, schemaFile.getAbsolutePath());
        } else {
            String cql = new String(Files.readAllBytes(schemaFile.toPath()));
            try {
                ParsedStatement statement = QueryProcessor.parseStatement(cql);
                if (statement instanceof CreateTableStatement.RawStatement) {
                    //CassandraUtils.cqlOverride = cql;
                    System.out.printf(IMPORTED_SCHEMA, schemaFile.getAbsolutePath());
                } else {
                    System.err.printf(FAILED_TO_IMPORT_SCHEMA, schemaFile.getAbsoluteFile(), "Wrong type of statement, " + statement.getClass());
                }
            } catch (SyntaxException se) {
                System.err.printf(FAILED_TO_IMPORT_SCHEMA, schemaFile.getAbsoluteFile(), se.getMessage());
            }
        }
	}
}
