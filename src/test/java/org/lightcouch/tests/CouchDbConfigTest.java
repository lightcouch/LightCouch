package org.lightcouch.tests;


import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.lightcouch.CouchDbProperties;

class CouchDbConfigTest {
    private static final Log log = LogFactory.getLog(CouchDbConfigTest.class);
    private static final String DEFAULT_FILE = "couchdb.properties";
    
    private Properties properties = new Properties();
    private String configFile;
    private CouchDbProperties dbProperties;

    public CouchDbConfigTest() {
        this(DEFAULT_FILE);
    }
    
    public CouchDbConfigTest(String configFile) {
        this.configFile = configFile;
        try {
            InputStream instream = CouchDbConfigTest.class.getClassLoader().getResourceAsStream(configFile);
            properties.load(instream);
        } catch (Exception e) {
            String msg = "Could not read configuration file from the classpath: " + configFile;
            log.error(msg);
            throw new IllegalStateException(msg, e);
        }
        readProperties();
    }
        
    private void readProperties() {
        try {
            // required
            dbProperties = new CouchDbProperties();
            dbProperties.setDbName(getProperty("couchdb.name", true));
            dbProperties.setCreateDbIfNotExist(new Boolean(getProperty("couchdb.createdb.if-not-exist", true)));
            dbProperties.setProtocol(getProperty("couchdb.protocol", true));
            dbProperties.setHost(getProperty("couchdb.host", true));
            dbProperties.setPort(Integer.parseInt(getProperty("couchdb.port", true)));
            dbProperties.setUsername(getProperty("couchdb.username", true));
            dbProperties.setPassword(getProperty("couchdb.password", true));
            
            // optional
            dbProperties.setPath(getProperty("couchdb.path", false));
            dbProperties.setSocketTimeout(getPropertyAsInt("couchdb.http.socket.timeout", false));
            dbProperties.setConnectionTimeout(getPropertyAsInt("couchdb.http.connection.timeout", false));
            dbProperties.setMaxConnections(getPropertyAsInt("couchdb.max.connections", false));
            dbProperties.setProxyHost(getProperty("couchdb.proxy.host", false));
            dbProperties.setProxyPort(getPropertyAsInt("couchdb.proxy.port", false));
            
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        properties = null;
    }
    
    public CouchDbProperties getProperties() {
        return dbProperties;
    }

    private String getProperty(String key, boolean isRequired) {
        String property = properties.getProperty(key);
        if(property == null && isRequired) {
            String msg = String.format("A required property is missing. Key: %s, File: %s", key, configFile);
            log.error(msg);
            throw new IllegalStateException(msg);
        } else {
            return (property != null && property.length() != 0) ? property.trim() : null;
        }
    }
    
    private int getPropertyAsInt(String key, boolean isRequired) {
        String prop = getProperty(key, isRequired);
        return (prop != null) ? Integer.parseInt(prop) : 0;
    }
}