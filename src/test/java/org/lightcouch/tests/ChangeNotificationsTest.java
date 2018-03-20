/*
 * Copyright (C) 2011 lightcouch.org
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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.Assume;
import org.junit.Test;
import org.lightcouch.Changes;
import org.lightcouch.ChangesResult;
import org.lightcouch.ChangesResult.Row;
import org.lightcouch.CouchDbInfo;
import org.lightcouch.Response;

import com.google.gson.JsonObject;

public class ChangeNotificationsTest extends CouchDbTestBase {
		
	@Test
	public void changes_normalFeed() {
		dbClient.save(new Foo()); 

		ChangesResult changes = dbClient.changes()
				.includeDocs(true)
				.limit(1)
				.getChanges();
		
		List<ChangesResult.Row> rows = changes.getResults();
		
		for (Row row : rows) {
			List<ChangesResult.Row.Rev> revs = row.getChanges();
			String docId = row.getId();
			JsonObject doc = row.getDoc();
			
			assertNotNull(revs);
			assertNotNull(docId);
			assertNotNull(doc);
		}
		
		assertThat(rows.size(), is(1));
	}
	
    @Test
    public void changes_normalFeed_seqInterval() {
        Assume.assumeTrue(isCouchDB2());

        dbClient.save(new Foo());
        dbClient.save(new Foo());
        dbClient.save(new Foo());
        dbClient.save(new Foo());
        dbClient.save(new Foo());

        ChangesResult changes = dbClient.changes().includeDocs(true).limit(5).seqInterval(2).getChanges();

        List<ChangesResult.Row> rows = changes.getResults();

        int seqs = 0;
        for (Row row : rows) {
            List<ChangesResult.Row.Rev> revs = row.getChanges();
            String docId = row.getId();
            JsonObject doc = row.getDoc();
            if (row.getSeq() != null)
                seqs++;

            assertNotNull(revs);
            assertNotNull(docId);
            assertNotNull(doc);
        }

        assertThat(rows.size(), is(5));
        assertThat(seqs, is(2));
    }

	@Test
	public void changes_normalFeed_selector() {
	    
	    Assume.assumeTrue(isCouchDB2());
	    
		dbClient.save(new Foo());
		ChangesResult changes = dbClient.changes().includeDocs(true).limit(1)
				.selector("{\"selector\":{\"_id\": {\"$gt\": null}}}").getChanges();

		List<ChangesResult.Row> rows = changes.getResults();

		for (Row row : rows) {
			List<ChangesResult.Row.Rev> revs = row.getChanges();
			String docId = row.getId();
			JsonObject doc = row.getDoc();

			assertNotNull(revs);
			assertNotNull(docId);
			assertNotNull(doc);
		}

		assertThat(rows.size(), is(1));
	}
	
	@Test
	public void changes_continuousFeed() {
		dbClient.save(new Foo()); 

		CouchDbInfo dbInfo = dbClient.context().info();
		String since = dbInfo.getUpdateSeq();

		Changes changes = dbClient.changes()
				.includeDocs(true)
				.since(since)
				.heartBeat(2000)
				.continuousChanges();

		Response response = dbClient.save(new Foo());

		while (changes.hasNext()) {
			ChangesResult.Row feed = changes.next();
			final JsonObject feedObject = feed.getDoc();
			final String docId = feed.getId();

			assertEquals(response.getId(), docId);
			assertNotNull(feedObject);

			changes.stop();
		}
	}
	
	@Test
	public void changes_continuousFeed_selector() {
	    
	    Assume.assumeTrue(isCouchDB2());
	    
		dbClient.save(new Foo());

		CouchDbInfo dbInfo = dbClient.context().info();
		String since = dbInfo.getUpdateSeq();

		Changes changes = dbClient.changes().includeDocs(true).since(since).heartBeat(1000)
				.selector("{\"selector\":{\"_id\": {\"$gt\": null}}}").continuousChanges();

		Response response = dbClient.save(new Foo());

		while (changes.hasNext()) {
			ChangesResult.Row feed = changes.next();
			final JsonObject feedObject = feed.getDoc();
			final String docId = feed.getId();
			System.out.println("next()=" + docId);

			assertEquals(response.getId(), docId);
			assertNotNull(feedObject);

			changes.stop();
		}
	}
}
