package org.lightcouch.tests;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.lightcouch.CouchDbClient;

public class CouchDbTestBase {

    protected static CouchDbClient dbClient;
    protected static CouchDbConfigTest dbClientConfig; 

    @BeforeClass
    public static void setUpClass() {
        dbClient = new CouchDbClient();
        dbClientConfig = new CouchDbConfigTest();
    }

    @AfterClass
    public static void tearDownClass() {
        dbClient.context().deleteDB(dbClientConfig.getProperties().getDbName(), "delete database");
        dbClient.shutdown();
    }
    
    protected boolean isCouchDB2() {
        String version = dbClient.context().serverVersion();
        return version.startsWith("2");
    }
    
    protected boolean isCouchDB1() {
        String version = dbClient.context().serverVersion();
        return version.startsWith("0") || version.startsWith("1") ;
    }
}
