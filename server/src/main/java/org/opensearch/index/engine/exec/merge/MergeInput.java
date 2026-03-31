/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.index.engine.exec.WriterFileSet;
import java.util.Collections;
import java.util.List;

public class MergeInput {
    private final List<WriterFileSet> fileMetadataList;
    private final long writerGeneration;
    private final List<String> sortingFields;
    private final List<Boolean> reverseSorts;
    private final String indexName;

    public MergeInput(List<WriterFileSet> fileMetadataList, long writerGeneration, List<String> sortingFields, List<Boolean> reverseSorts, String indexName) {
        this.fileMetadataList = fileMetadataList;
        this.writerGeneration = writerGeneration;
        this.sortingFields = sortingFields != null ? sortingFields : Collections.emptyList();
        this.reverseSorts = reverseSorts != null ? reverseSorts : Collections.emptyList();
        this.indexName = indexName;
    }

    public List<WriterFileSet> getFileMetadataList() {
        return fileMetadataList;
    }

    public long getWriterGeneration() {
        return writerGeneration;
    }

    public List<String> getSortingFields() {
        return sortingFields;
    }

    public List<Boolean> getReverseSorts() {
        return reverseSorts;
    }

    public String getIndexName() {
        return indexName;
    }
}
