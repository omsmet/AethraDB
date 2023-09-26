package AethraDB.calcite.rules;

import AethraDB.calcite.operators.LogicalArrowTableScan;
import AethraDB.util.arrow.ArrowTable;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.immutables.value.Value;

import java.util.List;

import static org.apache.calcite.rel.core.TableScan.identity;

/**
 * Planner rule that converts a {@link LogicalTableScan} to an equivalent {@link LogicalArrowTableScan}.
 * The resulting {@link LogicalArrowTableScan} projects out all its columns.
 */
@Value.Enclosing
public class ArrowTableScanRule extends RelRule<ArrowTableScanRule.Config> {

    /**
     * Creates an instance of the {@link ArrowTableScanRule}.
     */
    protected ArrowTableScanRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        if (call.rels.length == 1) {
            // the ordinary variant
            final LogicalTableScan scan = call.rel(0);
            apply(call, scan);
        } else {
            throw new AssertionError();
        }
    }

    /**
     * Applies the translation from an {@link LogicalTableScan} into an {@link LogicalArrowTableScan}
     * on rule match.
     */
    protected void apply(RelOptRuleCall call, LogicalTableScan scan) {
        final RelOptTable table = scan.getTable();
        assert table.unwrap(ArrowTable.class) != null;

        // We return a new scan which is essentially an identity of the original table scan
        // which projects out all columns in the original order.
        final List<Integer> selectedColumns = identity(table);
        LogicalArrowTableScan newScan = LogicalArrowTableScan.create(scan.getCluster(), scan.getTable(),
                scan.getHints(), selectedColumns);

        call.transformTo(newScan);
    }

    /**
     * Config specification for the {@link ArrowTableScanRule}.
     */
    @Value.Immutable
    public interface Config extends RelRule.Config {

        /**
         * Config that matches a {@link LogicalTableScan}.
         */
        Config DEFAULT = ImmutableArrowTableScanRule.Config.builder()
                .operandSupplier(b0 -> b0.operand(LogicalTableScan.class).noInputs())
                .build();

        @Override
        default ArrowTableScanRule toRule() {
            return new ArrowTableScanRule(this);
        }
    }

}
