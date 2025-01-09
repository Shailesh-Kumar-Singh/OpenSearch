/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Represents the type of comparison to be performed on a dimension.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public enum DimensionDataType {
    LONG {
        @Override
        int compare(Long a, Long b) {
            if (a == null && b == null) {
                return 0;
            }
            if (b == null) {
                return -1;
            }
            if (a == null) {
                return 1;
            }
            return Long.compare(a, b);
        }

        @Override
        Long parse(String value) {
            return Long.parseLong(value);
        }
    },
    UNSIGNED_LONG {
        @Override
        int compare(Long a, Long b) {
            if (a == null && b == null) {
                return 0;
            }
            if (b == null) {
                return -1;
            }
            if (a == null) {
                return 1;
            }
            return Long.compareUnsigned(a, b);
        }

        @Override
        Long parse(String value) {
            return Long.parseUnsignedLong(value);
        }
    };

    abstract int compare(Long a, Long b);

    abstract Long parse(String value);
}
