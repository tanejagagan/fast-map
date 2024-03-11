package info.gtaneja;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FastMapV1Test {

    @Test
    public void testSum() {
        Field key = new Field("key",
                FieldType.nullable( new ArrowType.Utf8()),
                /*children*/null
        );
        Field value = new Field("value",
                FieldType.nullable(new ArrowType.Int(64, true)),
                /*children*/null
        );
        Schema schema = new Schema(asList(key, value), /*metadata*/ null);
        try(
                BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                VarCharVector keyVector = (VarCharVector) root.getVector("key");
                BigIntVector valueVector = (BigIntVector) root.getVector("value");
        ){
            root.allocateNew();
            keyVector.set(0, "delhi".getBytes());
            valueVector.set(0, 100);
            keyVector.set(1, "bhopal".getBytes());
            valueVector.set(1, 200);
            keyVector.set(2, "delhi".getBytes());
            valueVector.set(2, 500);
            root.setRowCount(3);

            FastMapV1 fastMapV1 = new FastMapV1();
            System.out.println(root.contentToTSVString());
            VectorSchemaRoot sums = fastMapV1.sum(root, allocator);
            System.out.println(sums.contentToTSVString());
            VarCharVector keys  = (VarCharVector) sums.getVector(0);
            BigIntVector s = (BigIntVector) sums.getVector(1);

            assertEquals ( sums.getRowCount(), 2);
            Map<String, Long> map = new HashMap<>();
            for (int i = 0 ; i < sums.getRowCount(); i ++){
                map.put( new String(keys.get(i)), s.get(i));
            }
            assertEquals ( map.size(), 2);
            assertEquals(map.get("delhi"), 600);
            assertEquals(map.get("bhopal"), 200);
            sums.close();

        }
    }
}
