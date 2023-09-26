package AethraDB.benchmarks.util;

import AethraDB.evaluation.codegen.infrastructure.context.CodeGenContext;

/**
 * Abstract class which should be extended by JMH benchmarks to be able to consume the result from
 * a generated query in AethraDB. Specifically, the consuming benchmark should be added to the
 * {@link CodeGenContext} of the query, and the root of the query should be set to the
 * {@link ResultConsumptionOperator} so that it will transfer the query result to the benchmark.
 */
public abstract class ResultConsumptionTarget {

    /**
     * Method that consumes a single long integer result value from a query.
     * @param value The value to be consumed.
     */
    public void consumeResultItem(long value) {

    }

    /**
     * Method that consumes a integer array result value from a query.
     * @param value The value to be consumed.
     */
    public void consumeResultItem(int[] value) {

    }

    /**
     * Method that consumes a long integer array result value from a query.
     * @param value The value to be consumed.
     */
    public void consumeResultItem(long[] value) {

    }

}
