package com.bumblebee.app;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * @author barala
 *
 */

public class SchemaTest {
	private static final String resourceSchemaPath = "/src/test/resources/schema/schema.cql"; 
	private static String fullSchemaPath;
	private static final String keyspace1 = "vnodetesting";
	private static final String keyspace2 = "barala";
	
	@BeforeClass
	public static void init(){
		String currentPath = System.getProperty("user.dir");
		fullSchemaPath=currentPath+resourceSchemaPath;
	}
	
	@Test
	public void testSchema(){
		try {
			List<String> lines = Files.readAllLines(Paths.get(fullSchemaPath), StandardCharsets.UTF_8);
			List<CFMetaData>allCfMetaData = new ArrayList<CFMetaData>();
			for(String line : lines){
				allCfMetaData.add(CassandraUtil.tableFromCQL(line));
			}
			assertTrue(allCfMetaData.get(0).ksName.equals(keyspace1));
			assertTrue(allCfMetaData.get(1).ksName.equals(keyspace2));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
