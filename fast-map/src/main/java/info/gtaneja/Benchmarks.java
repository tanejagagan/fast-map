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
import org.openjdk.jmh.annotations.*;

import static java.util.Arrays.asList;

public class Benchmarks {
    @Benchmark
    @Fork(value = 1, warmups = 1)
    public void sum(ExecutionPlan executionPlan) {
        FastMapV1 fastMapV1 = new FastMapV1();
        VectorSchemaRoot res = fastMapV1.sum(executionPlan.root, executionPlan.allocator);
        executionPlan.root.close();
        res.close();
    }

    @State(Scope.Benchmark)
    public static class ExecutionPlan {

        @Param({ "1"})
        public int iterations;

        public BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
        public VectorSchemaRoot root;
        @Setup(Level.Invocation)
        public void setUp() {
            int size = 100;
            Field key = new Field("key",
                    FieldType.nullable( new ArrowType.Utf8()),
                    /*children*/null
            );
            Field value = new Field("value",
                    FieldType.nullable(new ArrowType.Int(64, true)),
                    /*children*/null
            );
            Schema schema = new Schema(asList(key, value), /*metadata*/ null);
                    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                    VarCharVector keyVector = (VarCharVector) root.getVector("key");
                    BigIntVector valueVector = (BigIntVector) root.getVector("value");
                    root.allocateNew();
                    for (int i = 0 ; i < size; i ++) {
                        String s = i % 2 ==0 ? "delhi" : "bhopal";
                        keyVector.set(i,  s.getBytes());
                        valueVector.set(i, i);
                    }
                    root.setRowCount(size);
                    this.root = root;
        }
    }
}

