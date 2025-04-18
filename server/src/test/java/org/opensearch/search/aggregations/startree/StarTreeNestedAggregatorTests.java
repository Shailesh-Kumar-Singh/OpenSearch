/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.junit.After;
import org.junit.Before;
import org.opensearch.common.Rounding;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.composite101.Composite101Codec;
import org.opensearch.index.codec.composite912.datacube.startree.StarTreeDocValuesFormatTests;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.OrdinalDimension;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitAdapter;
import org.opensearch.index.fielddata.ordinals.GlobalOrdinalsBuilder;
import org.opensearch.index.mapper.*;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregatorTestCase;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.opensearch.search.aggregations.bucket.range.InternalRange;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.GlobalOrdinalsStringTermsAggregator;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;

import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

import static org.opensearch.common.util.FeatureFlags.STAR_TREE_INDEX;
import static org.opensearch.index.codec.composite912.datacube.startree.AbstractStarTreeDVFormatTests.topMapping;
import static org.opensearch.search.aggregations.AggregationBuilders.*;
import static org.opensearch.search.aggregations.AggregationBuilders.dateHistogram;

import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */


import static org.opensearch.search.aggregations.AggregationBuilders.count;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
import static org.opensearch.search.aggregations.AggregationBuilders.min;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;

public class StarTreeNestedAggregatorTests extends DateHistogramAggregatorTestCase {
    private static final String TIMESTAMP_FIELD = "@timestamp";
    private static final MappedFieldType TIMESTAMP_FIELD_TYPE = new DateFieldMapper.DateFieldType(TIMESTAMP_FIELD);

//    private static final String FIELD_NAME = "status";
//    private static final MappedFieldType NUMBER_FIELD_TYPE = new NumberFieldMapper.NumberFieldType(
//            FIELD_NAME,
//            NumberFieldMapper.NumberType.LONG
//    );

    private static final String KEYWORD_FIELD = "clientip";
    MappedFieldType KEYWORD_FIELD_TYPE = new KeywordFieldMapper.KeywordFieldType(KEYWORD_FIELD);

    final static String STATUS = "status";
    final static String SIZE = "size";
    private static final MappedFieldType STATUS_FIELD_TYPE = new NumberFieldMapper.NumberFieldType(
            STATUS,
            NumberFieldMapper.NumberType.LONG
    );
    private static final MappedFieldType SIZE_FIELD_TYPE = new NumberFieldMapper.NumberFieldType(SIZE, NumberFieldMapper.NumberType.LONG);

    final static String CLIENTIP = "clientip";
    private static final MappedFieldType CLIENTIP_FIELD_NAME = new KeywordFieldMapper.KeywordFieldType(CLIENTIP);
    private static final MappedFieldType SIZE_FIELD_NAME = new NumberFieldMapper.NumberFieldType(SIZE, NumberFieldMapper.NumberType.FLOAT);
    private static FeatureFlags.TestUtils.FlagWriteLock fflock = null;

    @Before
    public void setup() {
        fflock = new FeatureFlags.TestUtils.FlagWriteLock(STAR_TREE_INDEX);
//        FeatureFlags.initializeFeatureFlags(Settings.builder().put(FeatureFlags.STAR_TREE_INDEX, true).build());
    }

    @After
    public void teardown() throws IOException {
        fflock.close();
//        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    protected Codec getCodec() {
        final Logger testLogger = LogManager.getLogger(MetricAggregatorTests.class);
        MapperService mapperService;
        try {
            mapperService = StarTreeDocValuesFormatTests.createMapperService(NumericTermsAggregatorTests.getExpandedMapping(1, false));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Composite101Codec(Lucene101Codec.Mode.BEST_SPEED, mapperService, testLogger);
    }


    public void testStarTreeNestedAggregations() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setCodec(getCodec());
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);

        Random random = RandomizedTest.getRandom();
        int totalDocs = 100;

        long val;
        long date;
        List<Document> docs = new ArrayList<>();
        Set<Long> statusSet = new HashSet<>();
        // Index 100 random documents
        for (int i = 0; i < totalDocs; i++) {
            Document doc = new Document();
            if (true) {
                val = 99; //random.nextInt(100); // Random int between 0 and 99 for status
                statusSet.add(val);
                doc.add(new SortedNumericDocValuesField(STATUS, val));
            }
            if (true) {
                val = random.nextInt(100); // NumericUtils.doubleToSortableLong(random.nextInt(100) + 0.5f);
                doc.add(new SortedNumericDocValuesField(SIZE, val));
            }
            if (true) {
                val = random.nextInt(10); // Random strings for int between 0 and 9 for clientip
                doc.add(new SortedSetDocValuesField(KEYWORD_FIELD, new BytesRef(String.valueOf(val))));
                doc.add(new StringField(KEYWORD_FIELD, String.valueOf(val), Field.Store.NO));
            }
            date =  24 * 60 * 60 * 1000L; // Random date within 180 days
            doc.add(new SortedNumericDocValuesField(TIMESTAMP_FIELD, date));
            doc.add(new LongPoint(TIMESTAMP_FIELD, date));
            iw.addDocument(doc);
            docs.add(doc);
        }

        if (randomBoolean()) {
            iw.forceMerge(1);
        }
        iw.close();
        DirectoryReader ir = DirectoryReader.open(directory);
        LeafReaderContext context = ir.leaves().get(0);

        SegmentReader reader = Lucene.segmentReader(context.reader());
        IndexSearcher indexSearcher = newSearcher(wrapInMockESDirectoryReader(ir), false, false);
        CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();

        List<CompositeIndexFieldInfo> compositeIndexFields = starTreeDocValuesReader.getCompositeIndexFields();
        CompositeIndexFieldInfo starTree = compositeIndexFields.get(0);


        LinkedHashMap<Dimension, MappedFieldType> supportedDimensions = new LinkedHashMap<>();
        supportedDimensions.put(new NumericDimension(STATUS), STATUS_FIELD_TYPE);
        supportedDimensions.put(new NumericDimension(SIZE), SIZE_FIELD_TYPE);
        supportedDimensions.put(
                new DateDimension(TIMESTAMP_FIELD, List.of(
                        new DateTimeUnitAdapter(Rounding.DateTimeUnit.MONTH_OF_YEAR),
                        new DateTimeUnitAdapter(Rounding.DateTimeUnit.DAY_OF_MONTH)
                ), DateFieldMapper.Resolution.MILLISECONDS),
                new DateFieldMapper.DateFieldType(TIMESTAMP_FIELD)
        );
        supportedDimensions.put(new OrdinalDimension(KEYWORD_FIELD), KEYWORD_FIELD_TYPE);

//        Query query = new MatchAllDocsQuery();
//        QueryBuilder queryBuilder = null;

        // DATE_HISTO INSIDE RANGE
        DateHistogramAggregationBuilder dateHistogramAggregationBuilder =  dateHistogram("by_day").field(TIMESTAMP_FIELD).calendarInterval(DateHistogramInterval.DAY).subAggregation( sum("_sum").field(STATUS));
        RangeAggregationBuilder rb1 = range("range_agg").field(STATUS).addRange(10, 30).addRange(30, 50).subAggregation(dateHistogramAggregationBuilder);
//        testCase(indexSearcher, query, queryBuilder, rb1, starTree, supportedDimensions);

        // RANGE INSIDE DATE_HISTO
        RangeAggregationBuilder rb = range("range_agg").field(STATUS).addRange(0,100).subAggregation( sum("_sum").field(STATUS));
        TermsAggregationBuilder tb = terms("term_status").field(STATUS).subAggregation(sum("_sum").field(STATUS));
        TermsAggregationBuilder tb1 = terms("term_status").field(STATUS).subAggregation(rb);

        TermsAggregationBuilder tb2 = terms("terms_agg").field(KEYWORD_FIELD)
            .collectMode(Aggregator.SubAggCollectionMode.BREADTH_FIRST);
        DateHistogramAggregationBuilder db = dateHistogram("by_day").field(TIMESTAMP_FIELD).calendarInterval(DateHistogramInterval.DAY).subAggregation(rb);
//        System.out.println(docs);
        System.out.println("db" + db);
//        testCase(indexSearcher, query, queryBuilder, tb2, starTree, supportedDimensions);

        Query query = new MatchAllDocsQuery();
        QueryBuilder queryBuilder = null;
        TermsAggregationBuilder termsAggregationBuilder = terms("terms_agg").field(CLIENTIP)
            .collectMode(Aggregator.SubAggCollectionMode.BREADTH_FIRST);
//        testCase(indexSearcher, query, queryBuilder, termsAggregationBuilder, starTree, supportedDimensions);

        ValuesSourceAggregationBuilder[] aggBuilders = {
                sum("_sum").field(STATUS)
//                max("_max").field(STATUS),
//                min("_min").field(STATUS),
//                count("_count").field(STATUS)
        };
//        System.out.println();
        List<Supplier<ValuesSourceAggregationBuilder<?>>> aggregationSuppliers = List.of(
//            () -> terms("term_status").field(STATUS),
                () -> terms("term_keyword").field(KEYWORD_FIELD).collectMode(Aggregator.SubAggCollectionMode.BREADTH_FIRST),
                () -> terms("term_size").field(SIZE));
//                () -> range("range_agg").field(STATUS).addRange(10, 30).addRange(30, 50),
//                () -> dateHistogram("by_day").field(TIMESTAMP_FIELD).calendarInterval(DateHistogramInterval.DAY));

        // 3-LEVELS [BUCKET -> BUCKET -> METRIC]
        int i = 0;
        for (ValuesSourceAggregationBuilder aggregationBuilder : aggBuilders) {
            System.out.println("metric AGGS LOOP " + i++);
            query = new MatchAllDocsQuery();
            queryBuilder = null;
            for (Supplier<ValuesSourceAggregationBuilder<?>> outerSupplier : aggregationSuppliers) {
                for (Supplier<ValuesSourceAggregationBuilder<?>> innerSupplier : aggregationSuppliers) {

                    if(innerSupplier == outerSupplier) { continue; }
                    System.out.println("outerSupplier " + outerSupplier.toString());
                    System.out.println("innerSupplier " + innerSupplier);

                    ValuesSourceAggregationBuilder<?> inner = innerSupplier.get().subAggregation(aggregationBuilder);
                    ValuesSourceAggregationBuilder<?> outer = outerSupplier.get().subAggregation(inner);
//                    System.out.println(outer);
//                    System.out.println("DOCS are: " + docs);
                    System.out.println("QUERY: " + outer);
                    testCase(indexSearcher, query, queryBuilder, outer, starTree, supportedDimensions);
                    testCase(indexSearcher, query, queryBuilder, outer, starTree, supportedDimensions);

                    // Numeric-terms query with numeric terms aggregation
                    for (int cases = 0; cases < 2; cases++) {
                        System.out.println("case : " + cases);
                        String queryField;
                        long queryValue;
                        if (false) {
                            queryField = STATUS; // terms + terms fails
                            queryValue = random.nextInt(10);
                        } else {
                            queryField = SIZE; // range + range fails
                            queryValue = random.nextInt(10);
                        }
                        query = SortedNumericDocValuesField.newSlowExactQuery(queryField, queryValue);
                        queryBuilder = new TermQueryBuilder(queryField, queryValue);
//                        System.out.println("query val" + queryValue);
//                        System.out.println("query field" + queryField);
//                        System.out.println(query);
////                        System.out.println(outer);
//                        System.out.println(docs);
                        System.out.println("query: " + outer);
                        testCase(indexSearcher, query, queryBuilder, outer, starTree, supportedDimensions);
                    }
                    break;
                }
            }
        }
        ir.close();
        directory.close();
//
//
        // 4-LEVELS [BUCKET -> BUCKET -> BUCKET -> METRIC]
//        for (ValuesSourceAggregationBuilder aggregationBuilder : aggBuilders) {
//            query = new MatchAllDocsQuery();
//            queryBuilder = null;
//            for (Supplier<ValuesSourceAggregationBuilder<?>> outermostSupplier : aggregationSuppliers) {
//                for (Supplier<ValuesSourceAggregationBuilder<?>> middleSupplier : aggregationSuppliers) {
//                    for (Supplier<ValuesSourceAggregationBuilder<?>> innerSupplier : aggregationSuppliers) {
//
//                        ValuesSourceAggregationBuilder<?> innermost = innerSupplier.get().subAggregation(aggregationBuilder);
//                        ValuesSourceAggregationBuilder<?> middle = middleSupplier.get().subAggregation(innermost);
//                        ValuesSourceAggregationBuilder<?> outermost = outermostSupplier.get().subAggregation(middle);
//
//                        testCase(indexSearcher, query, queryBuilder, outermost, starTree, supportedDimensions);
//
//                        // Numeric-terms query with numeric terms aggregation
//                        for (int cases = 0; cases < 10; cases++) {
////                            System.out.println("case 4 levels: " + cases);
//                            String queryField;
//                            long queryValue;
//                            if (true) {
//                                queryField = STATUS;
//                                queryValue = random.nextInt(10);
//                            } else {
//                                queryField = SIZE;
//                                queryValue = random.nextInt(10);
//                            }
//                            query = SortedNumericDocValuesField.newSlowExactQuery(queryField, queryValue);
//                            queryBuilder = new TermQueryBuilder(queryField, queryValue);
////                            System.out.println(docs);
//                            testCase(indexSearcher, query, queryBuilder, outermost, starTree, supportedDimensions);
//                        }
//
//                    }
//                }
//            }
//        }


    }

    private void testCase(
            IndexSearcher indexSearcher,
            Query query,
            QueryBuilder queryBuilder,
            ValuesSourceAggregationBuilder<?> aggregationBuilder,
            CompositeIndexFieldInfo starTree,
            LinkedHashMap<Dimension, MappedFieldType> supportedDimensions
    ) throws IOException {

        if (aggregationBuilder instanceof TermsAggregationBuilder) {
            InternalTerms defaultAggregation = searchAndReduceStarTree(
                    createIndexSettings(),
                    indexSearcher,
                    query,
                    queryBuilder,
                    aggregationBuilder,
                    null,
                    null,
                    null,
                    DEFAULT_MAX_BUCKETS,
                    false,
                    null,
                    false,
                    STATUS_FIELD_TYPE,
                SIZE_FIELD_TYPE,
                TIMESTAMP_FIELD_TYPE,
                KEYWORD_FIELD_TYPE
            );
            System.out.println("======================================== star tree ========================================");
            InternalTerms starTreeAggregation = searchAndReduceStarTree(
                    createIndexSettings(),
                    indexSearcher,
                    query,
                    queryBuilder,
                    aggregationBuilder,
                    starTree,
                    supportedDimensions,
                    null,
                    DEFAULT_MAX_BUCKETS,
                    false,
                    null,
                    true,
                    STATUS_FIELD_TYPE,
                    SIZE_FIELD_TYPE,
                    TIMESTAMP_FIELD_TYPE,
                    KEYWORD_FIELD_TYPE
            );
            System.out.println(defaultAggregation + "  debugging in terms " + starTreeAggregation);
            assertEquals(defaultAggregation.getBuckets().size(), starTreeAggregation.getBuckets().size());
            assertEquals(defaultAggregation.getBuckets(), starTreeAggregation.getBuckets());
        } else if (aggregationBuilder instanceof DateHistogramAggregationBuilder) {
//            System.out.println("DEBUG " + aggregationBuilder);
//            InternalRange defaultAggregation1 = searchAndReduce(
//                createIndexSettings(),
//                indexSearcher,
//                query,
//                aggregationBuilder,
//                DEFAULT_MAX_BUCKETS,
//                false,
//                false,
//                STATUS_FIELD_TYPE,
//                SIZE_FIELD_NAME,
//                TIMESTAMP_FIELD_TYPE
//            );
            InternalDateHistogram defaultAggregation = searchAndReduceStarTree(
                    createIndexSettings(),
                    indexSearcher,
                    query,
                    queryBuilder,
                    aggregationBuilder,
                    null,
                    null,
                    null,
                    DEFAULT_MAX_BUCKETS,
                    false,
                    null,
                    false,
                    STATUS_FIELD_TYPE,
                    TIMESTAMP_FIELD_TYPE,
                SIZE_FIELD_TYPE,
                KEYWORD_FIELD_TYPE
            );
            InternalDateHistogram starTreeAggregation = searchAndReduceStarTree(
                    createIndexSettings(),
                    indexSearcher,
                    query,
                    queryBuilder,
                    aggregationBuilder,
                    starTree,
                    supportedDimensions,
                    null,
                    DEFAULT_MAX_BUCKETS,
                    false,
                    null,
                    true,
                    STATUS_FIELD_TYPE,
                    TIMESTAMP_FIELD_TYPE,
                SIZE_FIELD_TYPE,
                KEYWORD_FIELD_TYPE
            );
            System.out.println(defaultAggregation + " debugging in date_histo " + starTreeAggregation);


            assertEquals(defaultAggregation.getBuckets().size(), starTreeAggregation.getBuckets().size());
            assertEquals(defaultAggregation.getBuckets(), starTreeAggregation.getBuckets());

        } else if (aggregationBuilder instanceof RangeAggregationBuilder) {
            //System.out.println("DEBUG NEW" + aggregationBuilder);
//            InternalRange defaultAggregation1 = searchAndReduce(
//                createIndexSettings(),
//                indexSearcher,
//                query,
//                aggregationBuilder,
//                DEFAULT_MAX_BUCKETS,
//                false,
//                false,
//                STATUS_FIELD_TYPE,
//                SIZE_FIELD_NAME,
//                TIMESTAMP_FIELD_TYPE
//            );

            InternalRange defaultAggregation = searchAndReduceStarTree(
                    createIndexSettings(),
                    indexSearcher,
                    query,
                    queryBuilder,
                    aggregationBuilder,
                    null,
                    null,
                    null,
                    DEFAULT_MAX_BUCKETS,
                    false,
                    null,
                    false,
                    STATUS_FIELD_TYPE,
                SIZE_FIELD_TYPE,
                    TIMESTAMP_FIELD_TYPE
            );

            InternalRange starTreeAggregation = searchAndReduceStarTree(
                    createIndexSettings(),
                    indexSearcher,
                    query,
                    queryBuilder,
                    aggregationBuilder,
                    starTree,
                    supportedDimensions,
                    null,
                    DEFAULT_MAX_BUCKETS,
                    false,
                    null,
                    true,
                    STATUS_FIELD_TYPE,
                SIZE_FIELD_TYPE,
                    TIMESTAMP_FIELD_TYPE
            );


//            System.out.println(defaultAggregation + "  debugging in range  " + starTreeAggregation);
            //if(defaultAggregation.ge)
            assertEquals(defaultAggregation.getBuckets().size(), starTreeAggregation.getBuckets().size());
            assertEquals(defaultAggregation.getBuckets(), starTreeAggregation.getBuckets());

        }


    }

    public static XContentBuilder getExpandedMapping(int maxLeafDocs, boolean skipStarNodeCreationForStatusDimension) throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree1"); // Use the same name as the provided mapping
            b.field("type", "star_tree");
            b.startObject("config");
            b.field("max_leaf_docs", maxLeafDocs);
            if (skipStarNodeCreationForStatusDimension) {
                b.startArray("skip_star_node_creation_for_dimensions");
                b.value("status"); // Skip for "status" dimension
                b.endArray();
            }
            b.startArray("ordered_dimensions");
            b.startObject();
            b.field("name", "status");
            b.endObject();
            b.startObject();
            b.field("name", "size");
            b.endObject();
            b.startObject();
            b.field("name", TIMESTAMP_FIELD);
            b.startArray("calendar_intervals");
            b.value("month");
            b.value("day");
            b.endArray();
            b.endObject();
            b.endArray();
            b.startArray("metrics");
            b.startObject();
            b.field("name", "size");
            b.startArray("stats");
            b.value("sum");
            b.value("value_count");
            b.value("min");
            b.value("max");
            b.endArray();
            b.endObject();
            b.startObject();
            b.field("name", "status");
            b.startArray("stats");
            b.value("sum");
            b.value("value_count");
            b.value("min");
            b.value("max");
            b.endArray();
            b.endObject();
            b.endArray();
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject("@timestamp");
            b.field("type", "date");
            b.field("format", "strict_date_optional_time||epoch_second");
            b.endObject();
            b.startObject("message");
            b.field("type", "keyword");
            b.field("index", false);
            b.field("doc_values", false);
            b.endObject();
            b.startObject("clientip");
            b.field("type", "ip");
            b.endObject();
            b.startObject("request");
            b.field("type", "text");
            b.startObject("fields");
            b.startObject("raw");
            b.field("type", "keyword");
            b.field("ignore_above", 256);
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("status");
            b.field("type", "integer");
            b.endObject();
            b.startObject("size");
            b.field("type", "float");
            b.endObject();
            b.startObject("geoip");
            b.startObject("properties");
            b.startObject("country_name");
            b.field("type", "keyword");
            b.endObject();
            b.startObject("city_name");
            b.field("type", "keyword");
            b.endObject();
            b.startObject("location");
            b.field("type", "geo_point");
            b.endObject();
            b.endObject();
            b.endObject();
            b.endObject();
        });
    }
}

