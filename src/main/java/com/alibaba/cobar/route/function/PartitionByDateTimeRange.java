package com.alibaba.cobar.route.function;

import com.alibaba.cobar.config.model.rule.RuleAlgorithm;
import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.function.FunctionExpression;

import java.util.List;
import java.util.Map;

/**
 * partition by range with datetime data type in mysql or other database providers.
 * <p/>
 * Created by @yunshi@wacai.com on 7/16/14.
 */
public class PartitionByDateTimeRange extends FunctionExpression implements RuleAlgorithm {

    public PartitionByDateTimeRange(String functionName) {
        super(functionName, null);
    }

    public PartitionByDateTimeRange(String functionName, List<Expression> arguments) {
        super(functionName, arguments);
    }

    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        return null;
    }

    @Override
    public RuleAlgorithm constructMe(Object... objects) {
        return null;
    }


    @Override
    public void init() {
        initialize();
    }

    @Override
    public void initialize() {

    }

    @Override
    public Object evaluationInternal(Map<? extends Object, ? extends Object> parameters) {
        return calculate(parameters)[0];
    }

    @Override
    public Integer[] calculate(Map<? extends Object, ? extends Object> parameters) {
        return new Integer[0];
    }
}
