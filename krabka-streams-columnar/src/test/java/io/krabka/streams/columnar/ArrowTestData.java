package io.krabka.streams.columnar;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

final class ArrowTestData {
    private ArrowTestData() {
    }

    static VectorSchemaRoot transactions(BufferAllocator allocator, String[] users, long[] amounts) {
        var fields = List.of(
                new Field("user", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("amount", FieldType.nullable(new ArrowType.Int(64, true)), null));
        var root = ArrowBatchSupport.create(fields, users.length, allocator);
        var user = (VarCharVector) root.getVector("user");
        var amount = (BigIntVector) root.getVector("amount");
        for (int row = 0; row < users.length; row++) {
            user.setSafe(row, users[row].getBytes(StandardCharsets.UTF_8));
            amount.setSafe(row, amounts[row]);
        }
        ArrowBatchSupport.setValueCounts(root);
        return root;
    }
}
