package io.krabka.streams.columnar;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Stores each partition snapshot in one atomically replaced local file.
 *
 * <p>Snapshots live in {@code <directory>/partition-<n>.snapshot}. Saving writes a
 * temporary file and moves it over the target — atomically where the file system
 * supports it — so a crash mid-save leaves the previous snapshot intact. The file
 * format is versioned and validated; a corrupt or truncated file fails loading with
 * {@link ColumnarException} rather than restoring partial state.
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
    private final Path directory;

    /**
     * Creates a store rooted at a directory.
     *
     * <p>The directory is created on the first save; it does not need to exist yet.
     *
     * @param directory the directory the snapshot files live in
     */
    public FileColumnarStateStore(Path directory) {
        this.directory = Objects.requireNonNull(directory, "directory");
    }

    /**
     * Loads a partition's snapshot file.
     *
     * @param partition the logical partition number
     * @return operator name to snapshot bytes; empty when no file exists
     * @throws ColumnarException if the file exists but cannot be read or is corrupt
     */
    @Override
    public Map<String, byte[]> load(int partition) {
        var file = file(partition);
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
                var name = input.readUTF();
                int length = input.readInt();
                if (length < 0) {
                    throw new ColumnarException("negative state snapshot length");
                }
                var bytes = input.readNBytes(length);
                if (bytes.length != length) {
                    throw new ColumnarException("truncated state snapshot");
                }
                result.put(name, bytes);
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
     * Saves a partition's snapshots, replacing the previous file atomically.
     *
     * @param partition the logical partition number
     * @param snapshot operator name to snapshot bytes
     * @throws ColumnarException if the file cannot be written or moved into place
     */
    @Override
    public void save(int partition, Map<String, byte[]> snapshot) {
        try {
            Files.createDirectories(directory);
            var target = file(partition);
            var temporary = Files.createTempFile(directory, target.getFileName().toString(), ".tmp");
            try {
                try (var output = new DataOutputStream(Files.newOutputStream(temporary))) {
                    output.writeInt(VERSION);
                    output.writeInt(snapshot.size());
                    for (var entry : snapshot.entrySet()) {
                        output.writeUTF(entry.getKey());
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
        } catch (IOException error) {
            throw new ColumnarException("cannot save partition " + partition + " state", error);
        }
    }

    private Path file(int partition) {
        return directory.resolve("partition-" + partition + ".snapshot");
    }
}
