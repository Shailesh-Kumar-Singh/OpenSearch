/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.lucene.util.IntroSorter;
import org.opensearch.index.compositeindex.datacube.Dimension;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.IntFunction;

/**
 * Utility class for building star tree
 */
public class StarTreeDocumentsSorter {
    /**
     * Sort documents based on the dimension values off heap using intro sorter.
     */
    public static void sort(
        final int[] sortedDocIds,
        final int dimensionId,
        final int numDocs,
        final IntFunction<Long[]> dimensionsReader,
        final List<Dimension> dimensionsOrder
    ) {
        new IntroSorter() {
            private Long[] dimensions;

            @Override
            protected void swap(int i, int j) {
                int temp = sortedDocIds[i];
                sortedDocIds[i] = sortedDocIds[j];
                sortedDocIds[j] = temp;
            }

            @Override
            protected void setPivot(int i) {
                dimensions = dimensionsReader.apply(i);
            }

            @Override
            protected int comparePivot(int j) {
                Long[] currentDimensions = dimensionsReader.apply(j);
                for (int i = dimensionId + 1; i < dimensions.length; i++) {
                    Dimension dimension = dimensionsOrder.get(i);
                    Long dimensionValue = currentDimensions[i];
                    if (!Objects.equals(dimensions[i], dimensionValue)) {
                        Comparator<Long> comparator = dimension.comparator();
                        return comparator.compare(dimensions[i], dimensionValue);
                    }
                }
                return 0;
            }
        }.sort(0, numDocs);
    }
}
