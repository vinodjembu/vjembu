package com.datastax.search.fit;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.bdp.search.solr.FieldInputTransformer;

/**
 * UDTFieldInputTransformer help to extract UDT Raw values and add them as field
 * on current document
 * 
 * @author vinodjembu
 *
 */
public class UDTFieldInputTransformer extends FieldInputTransformer {

	private static final Logger LOGGER = LoggerFactory.getLogger(UDTFieldInputTransformer.class);
	
	//Schema Variables
	private static final String EMAIL_HIST = "email_hist";
	private static final String PHONE_HIST = "phn_hist";
	private static final String EMAIL_ADDR = "email_addr";
	private static final String PHONE_CONT_CD = "phn_country_cd";
	private static final String PHONE_NUM = "phn_num";
	
	private static final String COMMA = ",";
	private static final String SEMICOLON = ":";

	@Override
	public boolean evaluate(String field) {
		return field.equals(EMAIL_HIST) || field.equals(PHONE_HIST);
	}

	@Override
	public void addFieldToDocument(SolrCore core, IndexSchema schema, String key, Document doc, SchemaField fieldInfo,
			String fieldValue, DocumentHelper helper) throws IOException {

		try {
			String fieldName = fieldInfo.getName();

			// Extracting email_hist UDT values
			if (fieldName.equalsIgnoreCase(EMAIL_HIST)) {
				
				String[] UDTEmailCollection = fieldValue.split(COMMA);
				for (int i = 0; i < UDTEmailCollection.length; i++) {
					
					String[] emailvalues = UDTEmailCollection[i].split(SEMICOLON);
					LOGGER.info(" email_addr Value : " + emailvalues[0]);
					helper.addFieldToDocument(core, core.getLatestSchema(), key, doc,
							core.getLatestSchema().getFieldOrNull(EMAIL_ADDR), emailvalues[0]);
				}
			}

			if (fieldName.equalsIgnoreCase(PHONE_HIST)) {
				
				String[] UDTPhoneCollection = fieldValue.split(COMMA);
				// Extracting phn_hist collection UDT values
				for (int i = 0; i < UDTPhoneCollection.length; i++) {
					
					String[] phonevalues = UDTPhoneCollection[i].split(SEMICOLON);
					LOGGER.info(" Phone Value : " + phonevalues[1] + " , " + phonevalues[2] );
					helper.addFieldToDocument(core, core.getLatestSchema(), key, doc,
							core.getLatestSchema().getFieldOrNull(PHONE_CONT_CD), phonevalues[1]);
					helper.addFieldToDocument(core, core.getLatestSchema(), key, doc,
							core.getLatestSchema().getFieldOrNull(PHONE_NUM), phonevalues[2]);
				}
			}

		} catch (Exception ex) {
			LOGGER.error("Error Processing UDT Field -UDTFieldInputTransformer : ",ex.getMessage(),ex);
		}
	}

}
