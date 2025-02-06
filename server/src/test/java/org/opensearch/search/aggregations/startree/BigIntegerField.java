/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/
package org.opensearch.search.aggregations.startree;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.sandbox.document.BigIntegerPoint;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.index.document.SortedUnsignedLongDocValuesRangeQuery;
import org.opensearch.index.document.SortedUnsignedLongDocValuesSetQuery;

import java.math.BigInteger;
import java.util.Arrays;

import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.hasDecimalPart;
import static org.opensearch.index.mapper.NumberFieldMapper.NumberType.objectToUnsignedLong;

public final class BigIntegerField extends Field {

    private static final FieldType FIELD_TYPE = new FieldType();
    private static final FieldType FIELD_TYPE_STORED;
    public static final int BYTES = 16;

    static {
        FIELD_TYPE.setDimensions(1, 16);
        FIELD_TYPE.setDocValuesType(DocValuesType.SORTED_NUMERIC);
        FIELD_TYPE.freeze();

        FIELD_TYPE_STORED = new FieldType(FIELD_TYPE);
        FIELD_TYPE_STORED.setStored(true);
        FIELD_TYPE_STORED.freeze();
    }

    private final StoredValue storedValue;

    /**
     * Creates a new BigIntegerField, indexing the provided point, storing it as a DocValue, and optionally
     * storing it as a stored field.
     *
     * @param name field name
     * @param value the BigInteger value
     * @param stored whether to store the field
     * @throws IllegalArgumentException if the field name or value is null.
     */
    public BigIntegerField(String name, BigInteger value, Field.Store stored) {
        super(name, stored == Field.Store.YES ? FIELD_TYPE_STORED : FIELD_TYPE);
        fieldsData = value;
        if (stored == Field.Store.YES) {
            storedValue = new StoredValue(value.longValue());
        } else {
            storedValue = null;
        }
    }

    @Override
    public BytesRef binaryValue() {
        return pack((BigInteger) fieldsData);
    }

    @Override
    public StoredValue storedValue() {
        return storedValue;
    }

    @Override
    public void setLongValue(long value) {
        super.setLongValue(value);
        if (storedValue != null) {
            storedValue.setLongValue(value);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " <" + name + ':' + fieldsData + '>';
    }

    /**
     * Create a query for matching an exact long value.
     *
     * @param field field name. must not be {@code null}.
     * @param value exact value
     * @throws IllegalArgumentException if {@code field} is null.
     * @return a query matching documents with this exact value
     */
    public static Query newExactQuery(String field, BigInteger value) {
        return newRangeQuery(field, value, value);
    }

    /**
     * Create a range query for long values.
     *
     * <p>You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries) by setting
     * {@code lowerValue = Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG} or {@code upperValue = Numbers.MIN_UNSIGNED_LONG_VALUE_AS_LONG}.
     *
     * <p>Ranges are inclusive. For exclusive ranges, pass {@code Math.addExact(lowerValue, 1)} or
     * {@code Math.addExact(upperValue, -1)}.
     *
     * @param field field name. must not be {@code null}.
     * @param lowerValue lower portion of the range (inclusive).
     * @param upperValue upper portion of the range (inclusive).
     * @throws IllegalArgumentException if {@code field} is null.
     * @return a query matching documents within this range.
     */
    public static Query newRangeQuery(String field, BigInteger lowerValue, BigInteger upperValue) {

        PointRangeQuery.checkArgs(field, lowerValue, upperValue);
        Query pq = new PointRangeQuery(field, pack(lowerValue).bytes, pack(upperValue).bytes, 1) {
            @Override
            protected String toString(int dimension, byte[] value) {
                return BigIntegerPoint.decodeDimension(value, 0).toString();
            }
        };

        Query dvQuery = SortedUnsignedLongDocValuesRangeQuery.newSlowRangeQuery(field, lowerValue, upperValue);

        PointRangeQuery.checkArgs(field, lowerValue, upperValue);
        return new IndexOrDocValuesQuery(pq, dvQuery);

    }

    private static BytesRef pack(BigInteger... point) {
        if (point == null) {
            throw new IllegalArgumentException("point must not be null");
        }
        if (point.length == 0) {
            throw new IllegalArgumentException("point must not be 0 dimensions");
        }
        byte[] packed = new byte[point.length * BYTES];

        for (int dim = 0; dim < point.length; dim++) {
            encodeDimension(point[dim], packed, dim * BYTES);
        }

        return new BytesRef(packed);
    }

    /** Encode single BigInteger dimension */
    public static void encodeDimension(BigInteger value, byte[] dest, int offset) {
        NumericUtils.bigIntToSortableBytes(value, BYTES, dest, offset);
    }

    /** Decode single BigInteger dimension */
    public static BigInteger decodeDimension(byte[] value, int offset) {
        return NumericUtils.sortableBytesToBigInt(value, offset, BYTES);
    }

    /**
     * Create a query matching values in a supplied set
     *
     * @param field field name. must not be {@code null}.
     * @param values long values
     * @throws IllegalArgumentException if {@code field} is null.
     * @return a query matching documents within this set.
     */
    public static Query newSetQuery(String field, BigInteger... values) {
        if (field == null) {
            throw new IllegalArgumentException("field cannot be null");
        }
        // Don't unexpectedly change the user's incoming values array:
        BigInteger[] sortedValues = values.clone();
        Arrays.sort(sortedValues);

        final BytesRef encoded = new BytesRef(new byte[16]);

        Query pointsQuery = new PointInSetQuery(field, 1, 16, new PointInSetQuery.Stream() {

            int upto;

            @Override
            public BytesRef next() {
                if (upto == sortedValues.length) {
                    return null;
                } else {
                    encodeDimension(sortedValues[upto], encoded.bytes, 0);
                    upto++;
                    return encoded;
                }
            }
        }) {
            @Override
            protected String toString(byte[] value) {
                assert value.length == BYTES;
                return decodeDimension(value, 0).toString();
            }
        };

        BigInteger[] v = new BigInteger[sortedValues.length];
        int upTo = 0;

        for (int i = 0; i < sortedValues.length; i++) {
            Object value = sortedValues[i];
            if (!hasDecimalPart(value)) {
                v[upTo++] = parse(value, true);
            }
        }
        if (upTo == 0) {
            return Queries.newMatchNoDocsQuery("All values have a decimal part");
        }
        if (upTo != v.length) {
            v = Arrays.copyOf(v, upTo);
        }
        Query dvq = SortedUnsignedLongDocValuesSetQuery.newSlowSetQuery(field, v);

        return new IndexOrDocValuesQuery(pointsQuery, dvq);
    }

    static public BigInteger parse(Object value, boolean coerce) {
        return objectToUnsignedLong(value, coerce);
    }

    /**
     * Create a new {@link SortField} for long values.
     *
     * @param field field name. must not be {@code null}.
     * @param reverse true if natural order should be reversed.
     * @param selector custom selector type for choosing the sort value from the set.
     */
    public static SortField newSortField(String field, boolean reverse, SortedNumericSelector.Type selector) {
        return new SortedNumericSortField(field, SortField.Type.LONG, reverse, selector);
    }

}
