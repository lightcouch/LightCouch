package org.lightcouch.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Assume;
import org.junit.Test;
import org.lightcouch.DocumentConflictException;
import org.lightcouch.NoDocumentException;
import org.lightcouch.Response;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class LocalDocumentsCRUDTest extends CouchDbTestBase {

    @Test
    public void findById() {
        Response response = dbClient.local().save(new Foo("test"));
        Foo foo = dbClient.local().find(Foo.class, response.getId());
        assertNotNull(foo);
        foo = dbClient.local().find(Foo.class, "test");
        assertNotNull(foo);
    }

    @Test
    public void findByIdContainSlash() {
        String generatedId = generateUUID() + "/" + generateUUID();
        Response response = dbClient.local().save(new Foo(generatedId));
        Foo foo = dbClient.local().find(Foo.class, response.getId());
        assertNotNull(foo);

        Foo foo2 = dbClient.local().find(Foo.class, generatedId);
        assertNotNull(foo2);
    }

    @Test
    public void findJsonObject() {
        Response response = dbClient.local().save(new Foo());
        JsonObject jsonObject = dbClient.local().find(JsonObject.class, response.getId());
        assertNotNull(jsonObject);

        JsonObject jsonObject2 = dbClient.local().find(response.getId());
        assertNotNull(jsonObject2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void findWithInvalidId_throwsIllegalArgumentException() {
        dbClient.local().find(Foo.class, "");
    }

    @Test(expected = NoDocumentException.class)
    public void findWithUnknownId_throwsNoDocumentException() {
        dbClient.local().find(Foo.class, generateUUID());
    }

    @Test
    public void contains() {
        Response response = dbClient.local().save(new Foo());
        boolean found = dbClient.local().contains(response.getId());
        assertTrue(found);

        found = dbClient.local().contains(generateUUID());
        assertFalse(found);
    }

    // Save

    @Test
    public void savePOJO() {
        Response response = dbClient.local().save(new Foo());
        assertNotNull(response.getId());
    }

    @Test
    public void saveMap() {

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("_id", generateUUID());
        map.put("field1", "value1");
        Response response = dbClient.local().save(map);
        assertNotNull(response.getId());
    }

    @Test
    public void saveJsonObject() {
        JsonObject json = new JsonObject();
        json.addProperty("_id", generateUUID());
        json.add("json-array", new JsonArray());
        Response response = dbClient.local().save(json);
        assertNotNull(response.getId());
    }

    @Test
    public void saveWithIdContainSlash() {
        String idWithSlash = "a/b/" + generateUUID();
        Response response = dbClient.local().save(new Foo(idWithSlash));
        assertEquals("_local/" + idWithSlash, response.getId());
    }

    @Test(expected = IllegalArgumentException.class)
    public void saveInvalidObject_throwsIllegalArgumentException() {
        dbClient.save(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void saveNewDocWithRevision_throwsIllegalArgumentException() {
        Bar bar = new Bar();
        bar.setRevision("unkown");
        dbClient.save(bar);
    }

    @Test
    public void saveDocWithDuplicateId_allowed() {
        Assume.assumeTrue(isCouchDB2());
        String id = generateUUID();
        dbClient.local().save(new Foo(id, "one"));
        Foo foo = dbClient.local().find(Foo.class, id);
        assertTrue("one".equals(foo.getTitle()));

        dbClient.local().save(new Foo(id, "two"));
        foo = dbClient.local().find(Foo.class, id);
        assertTrue("two".equals(foo.getTitle()));
    }

    @Test
    public void saveDocWithDuplicateId_throwsDocumentConflictException() {
        Assume.assumeTrue(isCouchDB1());
        boolean exceptionCatched = false;
        String id = generateUUID();
        dbClient.save(new Foo(id));
        try {
            dbClient.save(new Foo(id));
        } catch (DocumentConflictException e) {
            exceptionCatched = true;
        }
        assertTrue(exceptionCatched);
    }

    // Update

    @Test
    public void update() {
        Response response = dbClient.local().save(new Foo());
        Foo foo = dbClient.local().find(Foo.class, response.getId());
        response = dbClient.local().update(foo);
        assertNotNull(response.getId());
    }

    @Test
    public void updateWithIdContainSlash() {
        String idWithSlash = "a/" + generateUUID();
        Response response = dbClient.local().save(new Bar(idWithSlash));

        Bar bar = dbClient.local().find(Bar.class, response.getId());
        Response responseUpdate = dbClient.local().update(bar);
        assertEquals("_local/" + idWithSlash, responseUpdate.getId());
    }

    // Delete

    @Test
    public void deleteObjectV2() {
        Assume.assumeTrue(isCouchDB2());
        Response response = dbClient.local().save(new Foo());
        Foo foo = dbClient.local().find(Foo.class, response.getId());
        dbClient.local().remove(foo);
    }

    @Test
    public void deleteObjectV1() {
        Response response = dbClient.local().save(new Foo());
        Foo foo = dbClient.local().find(Foo.class, response.getId());
        dbClient.local().removeWithRev(foo);
    }

    @Test
    public void deleteById() {
        Assume.assumeTrue(isCouchDB2());
        Response response = dbClient.local().save(new Foo());
        response = dbClient.local().remove(response.getId());
        assertNotNull(response);
    }

    @Test
    public void deleteByIdAndRevValues() {
        Response response = dbClient.local().save(new Foo());
        response = dbClient.local().remove(response.getId(), response.getRev());
        assertNotNull(response);
    }

    @Test
    public void deleteByIdContainSlash() {
        Assume.assumeTrue(isCouchDB2());
        String idWithSlash = "a/" + generateUUID();
        Response response = dbClient.local().save(new Bar(idWithSlash));

        Response responseRemove = dbClient.local().remove(response.getId());
        assertEquals("_local/" + idWithSlash, responseRemove.getId());
    }

    @Test
    public void findAllLocalDocs() {
        Assume.assumeTrue(isCouchDB2());
        List<JsonObject> list = dbClient.local().findAll();
        int intialSize = list.size();
        dbClient.local().save(new Foo("test1"));
        dbClient.local().save(new Foo("test2"));
        dbClient.save(new Foo("test3"));

        list = dbClient.local().findAll();
        assertTrue(list.size() == intialSize + 2);
    }


    // Helper
    private static String generateUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }

}
