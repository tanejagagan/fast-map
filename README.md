## Objective 
    Improve the performance of sum aggregation. This can be extended for other aggregations such as count and average.
    FastMapV1 is implementation to find the sum based on a key. The implementation groups the values based on key and then add those values
    We need to implement FastMapV2 which is faster than FastMapV1 using quadratic probing.
    Few things which can improve the performance

- avoiding autoboxing ( https://www.theserverside.com/blog/Coffee-Talk-Java-News-Stories-and-Opinions/Performance-cost-of-Java-autoboxing-and-unboxing-of-primitive-types )
- vectorization. We use apache arrow (https://arrow.apache.org/cookbook/java/create.html#creating-vectors-arrays) 
- Hashmap implementation based on Quadratic probing (https://www.geeksforgeeks.org/quadratic-probing-in-hashing/)
- XXHash (https://xxhash.com/)



Running test
- `export _JAVA_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED"`
- `./mvnw test`

Running benchmark 
- `export _JAVA_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED"`
- `./mvnw compile exec:java -Dexec.mainClass="info.gtaneja.BenchmarkRunner"`