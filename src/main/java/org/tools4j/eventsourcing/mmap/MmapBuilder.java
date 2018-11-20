/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 tools4j, Marco Terzer, Anton Anufriev
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.tools4j.eventsourcing.mmap;

import org.tools4j.eventsourcing.api.IndexedPollerFactory;
import org.tools4j.eventsourcing.api.IndexedQueue;
import org.tools4j.mmap.region.api.RegionRingFactory;
import org.tools4j.mmap.region.impl.MappedFile;

import java.io.IOException;

public interface MmapBuilder {
    FilePrefixBuilder directory(String directory);

    interface FilePrefixBuilder {
        RegionRingFactoryBuilder filePrefix(String filePrefix);
    }

    interface RegionRingFactoryBuilder {
        Optionals regionRingFactory(RegionRingFactory regionRingFactory);
    }

    interface Optionals {
        Optionals clearFiles(boolean clearFiles);
        Optionals regionSize(int regionSize);
        Optionals regionRingSize(int regionRingSize);
        Optionals regionsToMapAhead(int regionsToMapAhead);
        Optionals maxFileSize(long maxFileSize);
        IndexedQueue buildQueue() throws IOException;
        IndexedQueue buildReadOnlyQueue() throws IOException;
        IndexedPollerFactory buildPollerFactory() throws IOException;
    }

    static MmapBuilder create() {
        return new DefaultMmapBuilder();
    }

    class DefaultMmapBuilder implements MmapBuilder, MmapBuilder.FilePrefixBuilder, MmapBuilder.RegionRingFactoryBuilder, MmapBuilder.Optionals {
        private String directory;
        private String filePrefix;
        private RegionRingFactory regionRingFactory;
        private boolean clearFiles = false;
        private int regionSize = (int) MappedFile.REGION_SIZE_GRANULARITY * 1024 * 4;
        private int regionRingSize = 4;
        private int regionsToMapAhead = 1;
        private long maxFileSize = 1024 * 1024 * 1024 * 2L;

        @Override
        public FilePrefixBuilder directory(final String directory) {
            this.directory = directory;
            return this;
        }

        @Override
        public RegionRingFactoryBuilder filePrefix(final String filePrefix) {
            this.filePrefix = filePrefix;
            return this;
        }

        @Override
        public Optionals regionRingFactory(final RegionRingFactory regionRingFactory) {
            this.regionRingFactory = regionRingFactory;
            return this;
        }

        @Override
        public Optionals clearFiles(final boolean clearFiles) {
            this.clearFiles = clearFiles;
            return this;
        }

        @Override
        public Optionals regionSize(final int regionSize) {
            this.regionSize = regionSize;
            return this;
        }

        @Override
        public Optionals regionRingSize(final int regionRingSize) {
            this.regionRingSize = regionRingSize;
            return this;
        }

        @Override
        public Optionals regionsToMapAhead(final int regionsToMapAhead) {
            this.regionsToMapAhead = regionsToMapAhead;
            return this;
        }

        @Override
        public Optionals maxFileSize(final long maxFileSize) {
            this.maxFileSize = maxFileSize;
            return this;
        }

        @Override
        public IndexedQueue buildQueue() throws IOException {
            return new MmapIndexedQueue(
                    directory,
                    filePrefix,
                    clearFiles,
                    regionRingFactory,
                    regionSize,
                    regionRingSize,
                    regionsToMapAhead,
                    maxFileSize);
        }

        @Override
        public IndexedQueue buildReadOnlyQueue() throws IOException {
            return new MmapReadOnlyIndexedQueue(
                    directory,
                    filePrefix,
                    regionRingFactory,
                    regionSize,
                    regionRingSize,
                    regionsToMapAhead);
        }

        @Override
        public IndexedPollerFactory buildPollerFactory() throws IOException {
            return new MmapIndexedPollerFactory(
                    directory,
                    filePrefix,
                    regionRingFactory,
                    regionSize,
                    regionRingSize,
                    regionsToMapAhead);
        }
    }

}
