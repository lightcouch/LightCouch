/*
 * Copyright (C) 2019 Indaba Consultores SL
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.lightcouch.tests;

import org.junit.Assert;
import org.junit.Test;
import org.lightcouch.DocumentNotModifiedException;
import org.lightcouch.Response;

import java.util.UUID;

public class ETagSupportTest extends CouchDbTestBase {

    private static String generateUUID() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    @Test
    public void testFindIfModified() {
        Bar doc = new Bar();
        String initialBar = "Initial";
        doc.setBar(initialBar);
        doc.setId(generateUUID());
        Response response = dbClient.save(doc);
        Bar initial = dbClient.find(Bar.class, response.getId());
        Assert.assertEquals(initialBar, initial.getBar());
        try {
            dbClient.findIfModified(Bar.class, initial.getId(), initial.getRevision());
            Assert.fail("Should not return if document has not been modified.");
        } catch (DocumentNotModifiedException e) {
            Assert.assertTrue(true);
        }
        String updatedBar = "Updated";
        initial.setBar(updatedBar);
        dbClient.update(initial);
        try {
            Bar updated = dbClient.findIfModified(Bar.class, initial.getId(), initial.getRevision());
            Assert.assertEquals(updatedBar, updated.getBar());
        } catch (DocumentNotModifiedException e) {
            Assert.fail("Should correctly return object if document has been modified.");
        }
    }
}
