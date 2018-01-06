package org.lightcouch.tests;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Test;
import org.lightcouch.Document;
import org.lightcouch.Page;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class PageAnnotationTest {

	@Test
	public void properJsonSerialization() {
		Page<Object> page = new Page<Object>();
		page.setHasNext(true);
		page.setHasPrevious(true);
		page.setNextParam("this should not be serialized");
		page.setPageNumber(42);
		page.setPreviousParam("this also not");
		page.setResultFrom(1000);
		page.setResultList(Collections.singletonList(new Document()));
		page.setResultTo(2000);
		page.setTotalResults(100000);
		
		Gson gson = new GsonBuilder()
				.create();
		
		String json = gson.toJson(page);
		
		assertTrue(json.contains("has_next"));;
		assertTrue(json.contains("results"));
		assertFalse(json.contains("nextParam"));
	}
}
