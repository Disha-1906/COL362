package rel;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.NlsString;
import convention.PConvention;
import java.math.BigDecimal;
import java.util.List;

/*
 * PProjectFilter is a relational operator that represents a Project followed by a Filter.
 * You need to write the entire code in this file.
 * To implement PProjectFilter, you can extend either Project or Filter class.
 * Define the constructor accordinly and override the methods as required.
 */
public class PProjectFilter extends Project implements PRel {
    private final RexNode condition;

    public PProjectFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<? extends RexNode> projects, RexNode condition, RelDataType rowType) {
        super(cluster, traitSet, ImmutableList.of(), input, projects, rowType);
        this.condition = condition;
    }
    @Override
    public Project copy(RelTraitSet traitSet,RelNode input, List<RexNode> projects, RelDataType rowType){
        return new PProjectFilter(getCluster(), traitSet, input,projects,condition,rowType);
    }

//    @Override
//    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
//        return new PProjectFilter(getCluster(), traitSet, sole(inputs), getProjects(), condition);
//    }

    //    @Override
//    public RexNode getCondition() {
//        return condition;
//    }
    public String toString() {
        return "PProjectFilter";
    }
    private Object[] cached_row;
    private int flag = 0;

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProjectFilter");
        /* Write your code here */
        PRel child = (PRel) getInput();
        boolean f = child.open();
        return f;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProjectFilter");
        /* Write your code here */
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProjectFilter has next");
        /* Write your code here */
        PRel child = (PRel) getInput();
        boolean ans = false;
        while(child.hasNext() && ans==false){
            Object[] r = child.next();
            RexCall filter = (RexCall) condition;
            SqlOperator oper = filter.getOperator();
            List<RexNode> op = filter.getOperands();
            ans = compute(oper,op,r);
            if(ans==false) continue;
            List<RexNode> projects = getProjects();
            Object[] projectedRow = new Object[projects.size()];
            for (int i = 0; i < projects.size(); i++) {
                RexNode projectExpr = projects.get(i);
                Object value = compute_operand(projectExpr, r);
                projectedRow[i] = value;
            }
//            return projectedRow;
            cached_row = projectedRow;
            flag = 1;
            return true;
        }
        return false;
    }

    private BigDecimal convert_to_BigDecimal(Object val){
        if(val instanceof Integer || val instanceof Double || val instanceof Float) return new BigDecimal(val.toString());
        else if(val instanceof BigDecimal) return (BigDecimal) val;
        throw new IllegalArgumentException("Unsupported operand type: " + val.getClass());
    }

    private Object compute_operand(RexNode op, Object[] r){
        if(op instanceof RexInputRef){
            int i = ((RexInputRef) op).getIndex();
            if(i>=0 && i<r.length) return r[i];
            throw new IllegalArgumentException("Invalid column index: " + i);
        }
        else if(op instanceof RexCall){
            SqlOperator opr = ((RexCall) op).getOperator();
            List<RexNode> ops = ((RexCall) op).getOperands();
            BigDecimal ld = convert_to_BigDecimal(compute_operand(ops.get(0),r));
            BigDecimal rd = convert_to_BigDecimal(compute_operand(ops.get(1),r));
            switch (opr.getKind()){
                case PLUS: return ld.add(rd);
                case MINUS: return ld.subtract(rd);
                case TIMES: return ld.multiply(rd);
                case DIVIDE: return ld.divide(rd);
                default:  throw new IllegalArgumentException("Unsupported operator: " + opr.getKind());
            }
        }
        else if(op instanceof RexLiteral) return ((RexLiteral) op).getValue();
        return null;
    }

    private int compare(Object lval, Object rval){
        if(lval instanceof Integer || lval instanceof Double || lval instanceof Float || lval instanceof BigDecimal){
            BigDecimal ld = convert_to_BigDecimal(lval);
            return ((Comparable) ld).compareTo(rval);
        }
        else if(lval instanceof String){
            return ((Comparable) lval).compareTo(((NlsString)rval).getValue());
        }
        else if(lval instanceof Boolean){
            return (Boolean.compare((Boolean) lval, (Boolean) rval));
        }
        return -1;
    }

    private boolean compute(SqlOperator oper, List<RexNode> op, Object[]r){
        Object lval, rval;
        switch (oper.getKind()){
            case AND:
                return compute(((RexCall) op.get(0)).getOperator(), ((RexCall) op.get(0)).getOperands(), r) &&
                        compute(((RexCall) op.get(1)).getOperator(), ((RexCall) op.get(1)).getOperands(), r);
            case OR:
                return compute(((RexCall) op.get(0)).getOperator(), ((RexCall) op.get(0)).getOperands(), r) ||
                        compute(((RexCall) op.get(1)).getOperator(), ((RexCall) op.get(1)).getOperands(), r);
            case NOT:
                return !compute(((RexCall) op.get(0)).getOperator(), ((RexCall) op.get(0)).getOperands(), r);
            case EQUALS:
                lval = compute_operand(op.get(0), r);
                rval = compute_operand(op.get(1), r);
                return compare(lval, rval) == 0;
            case GREATER_THAN:
                lval = compute_operand(op.get(0), r);
                rval = compute_operand(op.get(1), r);
                return compare(lval, rval) > 0;
            case LESS_THAN:
                lval = compute_operand(op.get(0), r);
                rval = compute_operand(op.get(1), r);
                return compare(lval, rval) < 0;
            case GREATER_THAN_OR_EQUAL:
                lval = compute_operand(op.get(0), r);
                rval = compute_operand(op.get(1), r);
                return compare(lval, rval) >= 0;
            case LESS_THAN_OR_EQUAL:
                lval = compute_operand(op.get(0), r);
                rval = compute_operand(op.get(1), r);
                return compare(lval, rval) <= 0;
            default: throw new UnsupportedOperationException("Unsupported operator: " + oper.getKind());
        }
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProjectFilter");
        /* Write your code here */
        Object[] r;
        if(flag == 1){ r = cached_row; flag=0; }
        else{ r = ((PRel)getInput()).next(); }
//        if(r == null) return null;
//        List<RexNode> projects = getProjects();
//        Object[] projectedRow = new Object[projects.size()];
//        for (int i = 0; i < projects.size(); i++) {
//            RexNode projectExpr = projects.get(i);
//            Object value = compute_operand(projectExpr, r);
//            projectedRow[i] = value;
//        }
//        return projectedRow;
        return r;
    }


}