package org.tools4j.eventsourcing.mmap;

import org.tools4j.eventsourcing.api.IndexedPollerFactory;
import org.tools4j.eventsourcing.api.IndexedQueue;
import org.tools4j.eventsourcing.api.IndexedTransactionalQueue;
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
        Optionals encodingBufferSize(int encodingBufferSize);
        IndexedQueue buildQueue() throws IOException;
        IndexedQueue buildReadOnlyQueue() throws IOException;
        IndexedPollerFactory buildPollerFactory() throws IOException;
        IndexedTransactionalQueue buildTransactionalQueue() throws IOException;
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
        private int encodingBufferSize = 8 * 1024;

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
        public Optionals encodingBufferSize(final int encodingBufferSize) {
            this.encodingBufferSize = encodingBufferSize;
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
                    maxFileSize,
                    encodingBufferSize);
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

        @Override
        public IndexedTransactionalQueue buildTransactionalQueue() throws IOException {
            return new MmapIndexedTransactionalQueue(
                    directory,
                    filePrefix,
                    clearFiles,
                    regionRingFactory,
                    regionSize,
                    regionRingSize,
                    regionsToMapAhead,
                    maxFileSize,
                    encodingBufferSize);
        }
    }

}
