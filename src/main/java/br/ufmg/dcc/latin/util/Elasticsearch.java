package br.ufmg.dcc.latin.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;



public class Elasticsearch {
	public static void main(String args[]) {
		
		
			try {
				getPageviewsByItemId(args[0], args[1]);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		
	}
	
	public static void getPageviewsByItemId(String index, String fileName) throws IOException{
		
		File fout = new File(fileName);
		FileOutputStream fos = new FileOutputStream(fout);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		bw.write("item_id,count\n");

		
		
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "felipemoraes").build();
		TransportClient transportClient = new TransportClient(settings);
		Client client = transportClient.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
		
		TermsBuilder aggregation = AggregationBuilders
	            .terms("total-events-by-items")
	                .field("item_id")
	                .size(0)
	                .subAggregation(
	                    AggregationBuilders.sum("total-count")
	                        .field("count")
	                );
		SearchResponse sr = client
	            .prepareSearch(index)
	            .setTypes("item_metric")
	            .setQuery(
	                QueryBuilders.filteredQuery(
	                    QueryBuilders.matchAllQuery(),
	                    FilterBuilders.boolFilter()
	                        .must(
	                            FilterBuilders.termFilter("type", "VIEW")
	                        )
	                )
	            )
	            .addAggregation(aggregation)
	            .execute()
	            .actionGet();
		StringTerms aggregatorByItemId = sr.getAggregations().get("total-events-by-items");
		
		for (Terms.Bucket bucketItemId: aggregatorByItemId.getBuckets()) {
	        String itemId = bucketItemId.getKey();
	        
	        Sum aggregatorTotalCount = bucketItemId.getAggregations().get("total-count");
	        double count = aggregatorTotalCount.getValue();
	        
				
			bw.write(itemId + "," + count + "\n");
	    }
		bw.close();
		transportClient.close();
	}
}
