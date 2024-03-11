package info.gtaneja;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;

public class FastMapV1 {
    /**
     *
     * @param input input Vector
     * @param allocator allocator to allocate new vector
     * @return sum by key
     * This function is used for find the sum by a given key.
     * when input rows are [delhi, 100], [bhopal, 50], [delhi, 1000] will result in [delhi, 1100], [bhopal, 50]
     */
    public VectorSchemaRoot sum(VectorSchemaRoot input,
                                BufferAllocator allocator) {
        HashMap<String, Long> map = new HashMap<>();
        VarCharVector k = (VarCharVector) input.getVector("key");
        BigIntVector v = (BigIntVector) input.getVector("value");
        int rowCount = input.getRowCount();

        for (int i = 0; i < rowCount; i++) {
            byte[] key = k.get(i);
            Long value = v.get(i);
            map.merge(new String(key), value, Long::sum);
        }

        Field key = new Field("key",
                FieldType.nullable(new ArrowType.Utf8()),
                /*children*/null
        );
        Field sum = new Field("sum",
                FieldType.nullable(new ArrowType.Int(64, true)),
                /*children*/null
        );
        Schema schema = new Schema(asList(key, sum), /*metadata*/ null);

                VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);
                VarCharVector resultKeys = (VarCharVector) result.getVector("key");
                BigIntVector resultValues = (BigIntVector) result.getVector("sum");

            result.allocateNew();
            int i = 0;
            for (Map.Entry<String, Long> e : map.entrySet()) {
                resultKeys.set(i, e.getKey().getBytes());
                resultValues.set(i, e.getValue());
                i++;
            }
            result.setRowCount(map.size());
            return result;

    }
}
