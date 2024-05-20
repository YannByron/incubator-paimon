/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.manifest;

import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.format.FormatWriterFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.manifest.IndexManifestEntry.Identifier;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ObjectsFile;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PathFactory;
import org.apache.paimon.utils.VersionedObjectSerializer;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.deletionvectors.DeletionVectorsIndexFile.DELETION_VECTORS_INDEX;

/** Index manifest file. */
public class IndexManifestFile extends ObjectsFile<IndexManifestEntry> {

    private IndexManifestFile(
            FileIO fileIO,
            FormatReaderFactory readerFactory,
            FormatWriterFactory writerFactory,
            PathFactory pathFactory) {
        super(
                fileIO,
                new IndexManifestEntrySerializer(),
                readerFactory,
                writerFactory,
                pathFactory,
                null);
    }

    /** Write new index files to index manifest. */
    @Nullable
    public String writeIndexFiles(
            @Nullable String previousIndexManifest,
            List<IndexManifestEntry> newIndexFiles,
            boolean autoMergeForDV) {
        if (newIndexFiles.isEmpty()) {
            return previousIndexManifest;
        }

        List<IndexManifestEntry> entries =
                previousIndexManifest == null ? new ArrayList<>() : read(previousIndexManifest);
        Pair<List<IndexManifestEntry>, List<IndexManifestEntry>> previous =
                separateIndexEntries(entries);
        Pair<List<IndexManifestEntry>, List<IndexManifestEntry>> current =
                separateIndexEntries(newIndexFiles);

        // Step1: get the hash index files;
        List<IndexManifestEntry> indexEntries =
                mergeByIdentifier(previous.getLeft(), current.getLeft());

        // Step2: get the dv index files;
        if (autoMergeForDV) {
            indexEntries.addAll(mergeByIdentifier(previous.getRight(), current.getRight()));
        } else {
            indexEntries.addAll(mergeByFileName(previous.getRight(), current.getRight()));
        }

        return writeWithoutRolling(indexEntries);
    }

    private Pair<List<IndexManifestEntry>, List<IndexManifestEntry>> separateIndexEntries(
            List<IndexManifestEntry> indexFiles) {
        List<IndexManifestEntry> hashEntries = new ArrayList<>();
        List<IndexManifestEntry> dvEntries = new ArrayList<>();
        for (IndexManifestEntry entry : indexFiles) {
            if (entry.indexFile().indexType().equals(DELETION_VECTORS_INDEX)) {
                dvEntries.add(entry);
            } else {
                hashEntries.add(entry);
            }
        }
        return Pair.of(hashEntries, dvEntries);
    }

    private List<IndexManifestEntry> mergeByIdentifier(
            List<IndexManifestEntry> prevIndexFiles, List<IndexManifestEntry> newIndexFiles) {
        Map<Identifier, IndexManifestEntry> indexEntries =
                prevIndexFiles.stream()
                        .filter(entry -> entry.kind() == FileKind.ADD)
                        .collect(
                                Collectors.toMap(
                                        IndexManifestEntry::identifier, Function.identity()));
        for (IndexManifestEntry entry : newIndexFiles) {
            if (entry.kind() == FileKind.ADD) {
                indexEntries.put(entry.identifier(), entry);
            } else {
                indexEntries.remove(entry.identifier());
            }
        }
        return new ArrayList<>(indexEntries.values());
    }

    private List<IndexManifestEntry> mergeByFileName(
            List<IndexManifestEntry> prevIndexFiles, List<IndexManifestEntry> newIndexFiles) {
        Map<String, IndexManifestEntry> indexEntries = new LinkedHashMap<>();
        for (IndexManifestEntry entry : prevIndexFiles) {
            if (entry.kind() == FileKind.ADD) {
                indexEntries.put(entry.indexFile().fileName(), entry);
            }
        }

        for (IndexManifestEntry entry : newIndexFiles) {
            if (entry.kind() == FileKind.ADD) {
                indexEntries.put(entry.indexFile().fileName(), entry);
            } else {
                indexEntries.remove(entry.indexFile().fileName());
            }
        }
        return new ArrayList<>(indexEntries.values());
    }

    /** Creator of {@link IndexManifestFile}. */
    public static class Factory {

        private final FileIO fileIO;
        private final FileFormat fileFormat;
        private final FileStorePathFactory pathFactory;

        public Factory(FileIO fileIO, FileFormat fileFormat, FileStorePathFactory pathFactory) {
            this.fileIO = fileIO;
            this.fileFormat = fileFormat;
            this.pathFactory = pathFactory;
        }

        public IndexManifestFile create() {
            RowType schema = VersionedObjectSerializer.versionType(IndexManifestEntry.schema());
            return new IndexManifestFile(
                    fileIO,
                    fileFormat.createReaderFactory(schema),
                    fileFormat.createWriterFactory(schema),
                    pathFactory.indexManifestFileFactory());
        }
    }
}
