package br.ufmg.dcc.latin.util;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;



public class HelloWorld {
	public static void main(String args[]) {
		System.out.println("hello, world"); 
		
		Client client = new TransportClient()
			.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
		
		TermsBuilder aggregation = AggregationBuilders
	            .terms("total-events-by-items")
	                .field("item_id")
	                .size(10)
	                .order(Terms.Order.aggregation("total-count", false))
	                .subAggregation(
	                    AggregationBuilders.sum("total-count")
	                        .field("count")
	                );
		SearchResponse sr = client
                .prepareSearch("metrics")
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
            
            //topKContentSummaryStats.add(new ContentSummaryStats(itemId, count, rank++, eventType.toString()));
        }
		
	}
}
