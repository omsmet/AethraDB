package calcite.rules;

import calcite.operators.LogicalArrowTableScan;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;
import util.arrow.ArrowTable;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that converts a {@link LogicalProject} on a {@link LogicalTableScan} to a
 * {@link LogicalArrowTableScan} which an embeds a projection.
 */
@Value.Enclosing
public class ArrowTableScanProjectionRule extends RelRule<ArrowTableScanProjectionRule.Config> {

    /**
     * Creates an instance of the {@link ArrowTableScanProjectionRule}.
     */
    protected ArrowTableScanProjectionRule(Config config) {
        super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
        if (call.rels.length == 2) {
            // the ordinary variant
            final LogicalProject project = call.rel(0);
            final LogicalTableScan scan = call.rel(1);
            apply(call, project, scan);
        } else {
            throw new AssertionError();
        }
    }

    /**
     * Applies the projection embedding into an {@link LogicalArrowTableScan} on a match.
     */
    protected void apply(RelOptRuleCall call, LogicalProject project, LogicalTableScan scan) {
        final RelOptTable table = scan.getTable();
        assert table.unwrap(ArrowTable.class) != null;

        final List<Integer> selectedColumns = new ArrayList<>();
        final RexVisitorImpl<Void> visitor = new RexVisitorImpl<Void>(true) {
            @Override public Void visitInputRef(RexInputRef inputRef) {
                if (!selectedColumns.contains(inputRef.getIndex())) {
                    selectedColumns.add(inputRef.getIndex());
                }
                return null;
            }
        };
        visitor.visitEach(project.getProjects());

        LogicalArrowTableScan newScan = LogicalArrowTableScan.create(scan.getCluster(), scan.getTable(),
                        scan.getHints(), selectedColumns);
        Mapping mapping = Mappings.target(selectedColumns, scan.getRowType().getFieldCount());
        final List<RexNode> newProjectRexNodes = RexUtil.apply(mapping, project.getProjects());

        if (RexUtil.isIdentity(newProjectRexNodes, newScan.getRowType())) {
            call.transformTo(newScan);
        } else {
            call.transformTo(
                    call.builder()
                            .push(newScan)
                            .project(newProjectRexNodes)
                            .build());
        }
    }

    /**
     * Config specification for the {@link ArrowTableScanProjectionRule}.
     */
    @Value.Immutable
    public interface Config extends RelRule.Config {

        /**
         * Config that matches a LogicalProject on LogicalTableScan.
         */
        Config DEFAULT = ImmutableArrowTableScanProjectionRule.Config.builder()
                .operandSupplier(b0 ->
                        b0.operand(LogicalProject.class).oneInput(b1 ->
                                b1.operand(LogicalTableScan.class).noInputs()))
                .build();

        @Override
        default ArrowTableScanProjectionRule toRule() {
            return new ArrowTableScanProjectionRule(this);
        }
    }

}
