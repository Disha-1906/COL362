package rules;


import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;

import convention.PConvention;

import rel.PProjectFilter;
import rel.PTableScan;

import org.checkerframework.checker.nullness.qual.Nullable;


public class PRules {

    private PRules(){
    }

    public static final RelOptRule P_TABLESCAN_RULE = new PTableScanRule(PTableScanRule.DEFAULT_CONFIG);

    private static class PTableScanRule extends ConverterRule {

        public static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalTableScan.class,
                        Convention.NONE, PConvention.INSTANCE,
                        "PTableScanRule")
                .withRuleFactory(PTableScanRule::new);

        protected PTableScanRule(Config config) {
            super(config);
        }

        @Override
        public @Nullable RelNode convert(RelNode relNode) {

            TableScan scan = (TableScan) relNode;
            final RelOptTable relOptTable = scan.getTable();

            if(relOptTable.getRowType() == scan.getRowType()) {
                return PTableScan.create(scan.getCluster(), relOptTable);
            }

            return null;
        }
    }

    // Write a class PProjectFilterRule that converts a LogicalProject followed by a LogicalFilter to a single PProjectFilter node.

    // You can make any changes starting here.
    public static class PProjectFilterRule extends RelOptRule {
        public static final PProjectFilterRule INSTANCE = new PProjectFilterRule();
        public PProjectFilterRule(){ super(operand(LogicalProject.class, operand(LogicalFilter.class,any())), "PProjectFilterRule");}

        private PProjectFilter construct(LogicalProject project, LogicalFilter filter){
            HepRelVertex temp = (HepRelVertex) filter.getInput();
            if (temp.getCurrentRel() instanceof TableScan==false) {
                LogicalProject p = (LogicalProject) temp.getCurrentRel();
                temp = (HepRelVertex) p.getInput();
                LogicalFilter f = (LogicalFilter) temp.getCurrentRel();
                RelNode input = construct(p, f);
                return new PProjectFilter(project.getCluster(),project.getTraitSet(),input,project.getProjects(), filter.getCondition(), project.getRowType());
            }
            TableScan scan = (TableScan) temp.getCurrentRel();
            return new PProjectFilter(project.getCluster(),project.getTraitSet(),scan,project.getProjects(), filter.getCondition(),  project.getRowType());

        }
        @Override
        public void onMatch(RelOptRuleCall call){
            final LogicalFilter f = call.rel(1);
            final LogicalProject p = call.rel(0);
            PProjectFilter r = construct(p,f);
//                new PProjectFilter(p.getCluster(), p.getCluster().traitSet().replace(PConvention.INSTANCE),s,  p.getProjects(), f.getCondition(), p.getRowType());

//                System.out.println();
            call.transformTo(r);

        }
    }

}

