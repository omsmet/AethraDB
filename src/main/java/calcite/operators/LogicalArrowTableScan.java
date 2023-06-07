package calcite.operators;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.ImmutableIntList;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * A {@link LogicalArrowTableScan} reads all rows from a {@link RelOptTable}, while projecting
 * only the necessary columns. Effectively this class is a merge of
 * {@link org.apache.calcite.interpreter.Bindables.BindableTableScan} and
 * {@link org.apache.calcite.rel.logical.LogicalTableScan}.
 */
public final class LogicalArrowTableScan extends TableScan {

    /**
     * The projections applied by this logical arrow table scan.
     */
    public final ImmutableIntList projects;

    /**
     * Creates a {@link LogicalArrowTableScan}.
     * Use {@link #create} unless you know what you are doing.
     * @param projects The projections to apply by the operator. Can be null if all columns should be projected.
     */
    public LogicalArrowTableScan(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table, ImmutableIntList projects) {
        super(cluster, traitSet, hints, table);
        this.projects = projects;
    }

    /**
     * Creates a {@link LogicalArrowTableScan}.
     */
    public static LogicalArrowTableScan create(RelOptCluster cluster, RelOptTable relOptTable, List<RelHint> hints) {
        return create(cluster, relOptTable, hints, identity(relOptTable));
    }

    /**
     * Creates a {@link LogicalArrowTableScan}.
     */
    public static LogicalArrowTableScan create(RelOptCluster cluster, RelOptTable relOptTable,
                                               List<RelHint> hints, List<Integer> projects) {
        final Table table = relOptTable.unwrap(Table.class);
        final RelTraitSet traitSet =
                cluster.traitSetOf(Convention.NONE)
                        .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
                            if (table != null) {
                                return table.getStatistic().getCollations();
                            }
                            return ImmutableList.of();
                        });
        return new LogicalArrowTableScan(cluster, traitSet, hints, relOptTable, ImmutableIntList.copyOf(projects));
    }

    @Override public RelDataType deriveRowType() {
        final RelDataTypeFactory.Builder builder =
                getCluster().getTypeFactory().builder();
        final List<RelDataTypeField> fieldList =
                table.getRowType().getFieldList();
        for (int project : projects) {
            builder.add(fieldList.get(project));
        }
        return builder.build();
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .itemIf("projects", projects, !projects.equals(identity()));
    }

    @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
                                                          RelMetadataQuery mq) {
        boolean noPushing = projects.size() == table.getRowType().getFieldCount();
        RelOptCost cost = super.computeSelfCost(planner, mq);
        if (noPushing || cost == null) {
            return cost;
        }

        // Cost factor for pushing fields
        // The "+ 2d" on top and bottom keeps the function fairly smooth.
        double p = ((double) projects.size() + 2d)
                / ((double) table.getRowType().getFieldCount() + 2d);

        // Multiply the cost by a factor that makes a scan more attractive if
        // filters and projects are pushed to the table scan
        return cost.multiplyBy(p * 0.01d);
    }

}
