package io.krabka.streams.columnar;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Stores each partition snapshot in one atomically replaced local file.
 *
 * <p>The running state of a partition lives in
 * {@code <directory>/partition-<n>.snapshot} and a barrier cut's state lives in
 * {@code <directory>/partition-<n>-epoch-<e>.snapshot}. Saving writes a temporary file
 * and moves it over the target — atomically where the file system supports it — so a
 * crash mid-save leaves the previous snapshot intact. The file format is versioned and
 * validated; a corrupt or truncated file fails loading with {@link ColumnarException}
 * rather than restoring partial state.
 *
 * <p>The container is the layout the barrier design freezes for all three krabka
 * streams libraries: a big-endian {@code u32} version of {@code 1}, a {@code u32}
 * entry count, and then per entry a {@code u32} name length, the UTF-8 name, a
 * {@code u32} value length, and the value bytes. Entries are written in ascending byte
 * order of the name, so the same snapshot always produces the same bytes.
 *
 * <p>Use this store when each group member has its own local disk and partitions
 * return to the same member across restarts, or accept rebuilding state from source
 * topics after a member change.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var stateStore = new FileColumnarStateStore(Path.of("/var/lib/app/state"));
 * var runner = ColumnarRunner.group(
 *     topology, consumer, producer,
 *     ColumnarErrorPolicy.fail(), stateStore, new ColumnarMetrics());
 * }</pre>
 */
public final class FileColumnarStateStore implements ColumnarStateStore {
    private static final int VERSION = 1;
    private static final int DEFAULT_RETAINED_EPOCHS = 100;
    private final Path directory;
    private final int retainedEpochs;
    private final Duration retainedFor;
    private final Map<Long, Integer> retained = new TreeMap<>();

    /**
     * Creates a store rooted at a directory.
     *
     * <p>The directory is created on the first save; it does not need to exist yet.
     *
     * @param directory the directory the snapshot files live in
     */
    public FileColumnarStateStore(Path directory) {
        this(directory, DEFAULT_RETAINED_EPOCHS, null);
    }

    /**
     * Creates a store that keeps the newest {@code retainedEpochs} barrier snapshots
     * per partition.
     *
     * @param directory the directory the snapshot files live in
     * @param retainedEpochs the positive number of barrier epochs to retain
     */
    public FileColumnarStateStore(Path directory, int retainedEpochs) {
        this(directory, retainedEpochs, null);
    }

    /**
     * Creates a store that keeps barrier snapshots newer than {@code retainedFor}.
     *
     * @param directory the directory the snapshot files live in
     * @param retainedFor the positive snapshot retention duration
     */
    public FileColumnarStateStore(Path directory, Duration retainedFor) {
        this(directory, 0, Objects.requireNonNull(retainedFor, "retainedFor"));
        if (retainedFor.isZero() || retainedFor.isNegative()) {
            throw new IllegalArgumentException("retainedFor must be positive");
        }
    }

    private FileColumnarStateStore(Path directory, int retainedEpochs, Duration retainedFor) {
        this.directory = Objects.requireNonNull(directory, "directory");
        if (retainedFor == null && retainedEpochs < 1) {
            throw new IllegalArgumentException("retainedEpochs must be positive");
        }
        this.retainedEpochs = retainedEpochs;
        this.retainedFor = retainedFor;
    }

    /**
     * Loads a partition's snapshot file for an epoch.
     *
     * @param partition the logical partition number
     * @param epoch the barrier epoch, or {@link ColumnarStateStore#LIVE_EPOCH} for the
     *     running state
     * @return operator name to snapshot bytes; empty when no file exists
     * @throws ColumnarException if the file exists but cannot be read or is corrupt
     */
    @Override
    public synchronized Map<String, byte[]> load(int partition, long epoch) {
        var file = file(partition, epoch);
        if (!Files.exists(file)) {
            return Map.of();
        }
        try (var input = new DataInputStream(Files.newInputStream(file))) {
            if (input.readInt() != VERSION) {
                throw new ColumnarException("unsupported state snapshot version");
            }
            int count = input.readInt();
            if (count < 0) {
                throw new ColumnarException("negative state snapshot entry count");
            }
            var result = new LinkedHashMap<String, byte[]>();
            for (int index = 0; index < count; index++) {
                var name = new String(read(input, length(input)), StandardCharsets.UTF_8);
                result.put(name, read(input, length(input)));
            }
            if (input.read() != -1) {
                throw new ColumnarException("trailing bytes in state snapshot");
            }
            return java.util.Collections.unmodifiableMap(result);
        } catch (IOException error) {
            throw new ColumnarException("cannot load partition " + partition + " state", error);
        }
    }

    /**
     * Saves a partition's snapshots for an epoch, replacing the previous file
     * atomically.
     *
     * @param partition the logical partition number
     * @param epoch the barrier epoch, or {@link ColumnarStateStore#LIVE_EPOCH} for the
     *     running state
     * @param snapshot operator name to snapshot bytes
     * @throws ColumnarException if the file cannot be written or moved into place
     */
    @Override
    public synchronized void save(int partition, long epoch, Map<String, byte[]> snapshot) {
        try {
            Files.createDirectories(directory);
            deleteTemporaryFiles();
            var target = file(partition, epoch);
            var temporary = Files.createTempFile(directory, target.getFileName().toString(), ".tmp");
            try {
                try (var output = new DataOutputStream(Files.newOutputStream(temporary))) {
                    output.writeInt(VERSION);
                    output.writeInt(snapshot.size());
                    for (var entry : ordered(snapshot)) {
                        output.writeInt(entry.getKey().length);
                        output.write(entry.getKey());
                        output.writeInt(entry.getValue().length);
                        output.write(entry.getValue());
                    }
                }
                try {
                    Files.move(
                            temporary,
                            target,
                            StandardCopyOption.ATOMIC_MOVE,
                            StandardCopyOption.REPLACE_EXISTING);
                } catch (java.nio.file.AtomicMoveNotSupportedException ignored) {
                    Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
                }
            } finally {
                Files.deleteIfExists(temporary);
            }
            if (epoch != ColumnarStateStore.LIVE_EPOCH) {
                reclaim(partition);
            }
        } catch (IOException error) {
            throw new ColumnarException("cannot save partition " + partition + " state", error);
        }
    }

    @Override
    public synchronized java.util.Optional<List<Long>> retainedEpochs(int partition) {
        try {
            if (!Files.isDirectory(directory)) {
                return java.util.Optional.of(List.of());
            }
            try (var files = Files.list(directory)) {
                return java.util.Optional.of(files.map(Path::getFileName)
                        .map(Path::toString)
                        .map(name -> epoch(partition, name))
                        .flatMapToLong(java.util.OptionalLong::stream)
                        .sorted()
                        .boxed()
                        .toList());
            }
        } catch (IOException error) {
            throw new ColumnarException("cannot list partition " + partition + " state", error);
        }
    }

    @Override
    public synchronized EpochLease retain(long epoch) {
        retained.merge(epoch, 1, Integer::sum);
        return () -> release(epoch);
    }

    private synchronized void release(long epoch) {
        retained.computeIfPresent(epoch, (ignored, count) -> count == 1 ? null : count - 1);
    }

    private void reclaim(int partition) throws IOException {
        try (var files = Files.list(directory)) {
            var snapshots = files.filter(path -> epoch(partition, path.getFileName().toString()).isPresent())
                    .sorted((left, right) -> Long.compare(
                            epoch(partition, right.getFileName().toString()).orElseThrow(),
                            epoch(partition, left.getFileName().toString()).orElseThrow()))
                    .toList();
            if (retainedFor == null) {
                for (var stale : snapshots.subList(Math.min(retainedEpochs, snapshots.size()), snapshots.size())) {
                    if (!retained.containsKey(epoch(partition, stale.getFileName().toString()).orElseThrow())) {
                        Files.deleteIfExists(stale);
                    }
                }
            } else {
                var cutoff = java.nio.file.attribute.FileTime.from(java.time.Instant.now().minus(retainedFor));
                for (var stale : snapshots) {
                    if (Files.getLastModifiedTime(stale).compareTo(cutoff) < 0
                            && !retained.containsKey(epoch(partition, stale.getFileName().toString()).orElseThrow())) {
                        Files.deleteIfExists(stale);
                    }
                }
            }
        }
    }

    private void deleteTemporaryFiles() throws IOException {
        try (var files = Files.list(directory)) {
            for (var temporary : files.filter(path -> {
                        var name = path.getFileName().toString();
                        return name.startsWith("partition-") && name.endsWith(".tmp");
                    })
                    .toList()) {
                Files.deleteIfExists(temporary);
            }
        }
    }

    private static java.util.OptionalLong epoch(int partition, String name) {
        var prefix = "partition-" + partition + "-epoch-";
        var suffix = ".snapshot";
        if (!name.startsWith(prefix) || !name.endsWith(suffix)) {
            return java.util.OptionalLong.empty();
        }
        try {
            return java.util.OptionalLong.of(Long.parseLong(name.substring(prefix.length(), name.length() - suffix.length())));
        } catch (NumberFormatException ignored) {
            return java.util.OptionalLong.empty();
        }
    }

    private static List<Map.Entry<byte[], byte[]>> ordered(Map<String, byte[]> snapshot) {
        var entries = new ArrayList<Map.Entry<byte[], byte[]>>(snapshot.size());
        snapshot.forEach((name, value) -> entries.add(Map.entry(name.getBytes(StandardCharsets.UTF_8), value)));
        entries.sort((left, right) -> Arrays.compareUnsigned(left.getKey(), right.getKey()));
        return entries;
    }

    private static int length(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 0) {
            throw new ColumnarException("negative state snapshot length");
        }
        return length;
    }

    private static byte[] read(DataInputStream input, int length) throws IOException {
        var bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new ColumnarException("truncated state snapshot");
        }
        return bytes;
    }

    private Path file(int partition, long epoch) {
        return epoch == ColumnarStateStore.LIVE_EPOCH
                ? directory.resolve("partition-" + partition + ".snapshot")
                : directory.resolve("partition-" + partition + "-epoch-" + epoch + ".snapshot");
    }
}
