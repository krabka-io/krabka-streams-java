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

/** Stores each partition snapshot in one atomically replaced local file. */
public final class FileColumnarStateStore implements ColumnarStateStore {
    private static final int VERSION = 1;
    private final Path directory;

    public FileColumnarStateStore(Path directory) {
        this.directory = Objects.requireNonNull(directory, "directory");
    }

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
