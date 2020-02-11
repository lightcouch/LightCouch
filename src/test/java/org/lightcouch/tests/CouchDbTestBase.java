package org.lightcouch.tests;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.lightcouch.CouchDbClient;

import com.github.zafarkhaja.semver.Version;

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

    protected boolean isCouchDB23() {
        return isCouchDBVersion(">=2.3.0");
    }
    
    protected boolean isCouchDB2() {
        return isCouchDBVersion(">=2.0.0");
    }
    
    protected boolean isCouchDB1() {
        return isCouchDBVersion(">=0.0.0 & <2.0.0"); 
    }
    
    protected boolean isCouchDBVersion(String versionExpression) {
        String version = dbClient.context().serverVersion();
        Version serverVersion = Version.valueOf(version);
        return serverVersion.satisfies(versionExpression);
    }
}
