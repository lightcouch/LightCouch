package org.lightcouch.tests;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assume;
import org.junit.Test;
import org.lightcouch.CouchDbException;
import org.lightcouch.PurgeResponse;
import org.lightcouch.Response;

import junit.framework.Assert;

public class PurgeTest extends CouchDbTestBase {

    @Test
    public void pre23NotsupportedTest() {
        Assume.assumeTrue(isCouchDB2() && !isCouchDB23());
        Map<String, List<String>> toPurge = new HashMap<String, List<String>>();
        toPurge.put("222", Arrays.asList("1-967a00dff5e02add41819138abb3284d"));
        try {
            dbClient.purge(toPurge);
            Assert.fail("Exception is expected");
        } catch (CouchDbException e) {
            Assert.assertTrue(e.getMessage().startsWith("Not Implemented"));
        }
    }

    @Test
    public void nullMapNotSupportedTest() {
        try {
            dbClient.purge(null);
            Assert.fail("Exception is expected");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().equals("to purge map may not be null."));
        }
    }

    @Test
    public void emptyMapNotSupportedTest() {
        Map<String, List<String>> toPurge = new HashMap<String, List<String>>();
        try {
            dbClient.purge(toPurge);
            Assert.fail("Exception is expected");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().equals("to purge map may not be empty."));
        }
    }

    @Test
    public void purgeTest() {
        Assume.assumeTrue(isCouchDB23());
        
        Response creation = dbClient.save(new Foo("111"));
        String rev = creation.getRev();
        
        Assert.assertTrue(dbClient.contains("111"));
       
        Map<String, List<String>> toPurge = new HashMap<String, List<String>>();
        
        toPurge.put("111", Arrays.asList(rev));
        PurgeResponse response =  dbClient.purge(toPurge);
        Assert.assertTrue(response.getPurged().containsKey("111"));
        List<String> revs = response.getPurged().get("111");
        Assert.assertTrue(revs.contains(rev));
        
        Assert.assertTrue(!dbClient.contains("111"));
    }
    
    @Test
    public void purgeNonExistingDocTest() {
        Assume.assumeTrue(isCouchDB23());
                
        Assert.assertTrue(!dbClient.contains("222"));
       
        Map<String, List<String>> toPurge = new HashMap<String, List<String>>();
        
        toPurge.put("222", Arrays.asList("1-967a00dff5e02add41819138abb3284d"));
        PurgeResponse response =  dbClient.purge(toPurge);
        Assert.assertTrue(response.getPurged().containsKey("222"));
        Assert.assertTrue(response.getPurged().get("222").isEmpty());
        
    }

}
