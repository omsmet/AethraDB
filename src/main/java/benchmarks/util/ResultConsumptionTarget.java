package benchmarks.util;

/**
 * Interface which should be implemented by JMH benchmarks to be able to consume the result from
 * a generated query in AethraDB. Specifically, the consuming benchmark should be added to the
 * {@link evaluation.codegen.infrastructure.context.CodeGenContext} of the query, and the root of
 * the query should be set to the {@link ResultConsumptionOperator} so that it will transfer the
 * query result to the benchmark.
 */
public interface ResultConsumptionTarget {

    /**
     * Method that consumes a single long integer result value from a query.
     * Method can be called multiple times to consume a query result consisting of multiple values.
     * @param value The value to be consumed.
     */
    void consumeResultItem(long value);

}
