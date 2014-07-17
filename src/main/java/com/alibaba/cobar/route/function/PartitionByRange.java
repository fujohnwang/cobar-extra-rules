package com.alibaba.cobar.route.function;

import com.alibaba.cobar.config.model.rule.RuleAlgorithm;
import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.function.FunctionExpression;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * rule for range based on long number
 * <p/>
 * just borrow ideas from @{com.alibaba.cobar.route.function.PartitionByFileMap}
 *
 * @author yunshi@wacai.com
 */
public class PartitionByRange extends FunctionExpression implements RuleAlgorithm {


    private String rangeDefinitionFile;

    private Map<Integer, ImmutablePair<Long, Long>> mapping;

    public PartitionByRange(String functionName) {
        super(functionName, null);
    }

    public PartitionByRange(String functionName, List<Expression> arguments) {
        super(functionName, arguments);
    }

    @Override
    public Object evaluationInternal(Map<? extends Object, ? extends Object> parameters) {
        return calculate(parameters)[0];
    }

    @Override
    public void init() {
        initialize();
    }

    @Override
    public Integer[] calculate(Map<? extends Object, ? extends Object> parameters) {
        Integer[] rst = new Integer[1];
        Object arg = arguments.get(0).evaluation(parameters);
        if (arg == null) {
            throw new IllegalArgumentException("partition key is null ");
        } else if (arg == UNEVALUATABLE) {
            throw new IllegalArgumentException("argument is UNEVALUATABLE");
        }
        Number key;
        if (arg instanceof Number) {
            key = (Number) arg;
        } else if (arg instanceof String) {
            key = Long.parseLong((String) arg);
        } else {
            throw new IllegalArgumentException("unsupported data type for partition key: " + arg.getClass());
        }


        for (Map.Entry<Integer, ImmutablePair<Long, Long>> entry : mapping.entrySet()) {
            if (key.longValue() >= entry.getValue().getLeft() && key.longValue() < entry.getValue().getRight()) {
                rst[0] = entry.getKey();
                break;
            }
        }
        return rst;
    }


    @Override
    public FunctionExpression constructFunction(List<Expression> arguments) {
        if (arguments == null || arguments.size() != 1)
            throw new IllegalArgumentException("function " + getFunctionName() + " must have 1 argument but is "
                    + arguments);
        Object[] args = new Object[arguments.size()];
        int i = -1;
        for (Expression arg : arguments) {
            args[++i] = arg;
        }

        return (FunctionExpression) constructMe(args);
    }

    public RuleAlgorithm constructMe(Object... objects) {
        List<Expression> args = new ArrayList<Expression>(objects.length);
        for (Object obj : objects) {
            args.add((Expression) obj);
        }
        PartitionByRange func = new PartitionByRange(functionName, args);
        func.rangeDefinitionFile = rangeDefinitionFile;
        return func;
    }

    @Override
    public void initialize() {
        InputStream fin = null;
        try {
            fin = new FileInputStream(new File(getRangeDefinitionFile()));
            BufferedReader in = new BufferedReader(new InputStreamReader(fin));
            mapping = new HashMap<Integer, ImmutablePair<Long, Long>>();
            for (String line; (line = in.readLine()) != null; ) {
                line = line.trim();
                if (line.startsWith("#") || line.startsWith("//") || !StringUtils.trimToEmpty(line).contains("="))
                    continue;
                try {
                    String key = StringUtils.trimToEmpty(StringUtils.substringBefore(line, "="));
                    int shardIndex = Integer.parseInt(StringUtils.trimToEmpty(StringUtils.substringAfter(line, "=")));

                    if (StringUtils.contains(key, ",")) {
                        String open = StringUtils.trimToEmpty(StringUtils.substringBefore(key, ","));
                        String end = StringUtils.trimToEmpty(StringUtils.substringAfter(key, ","));
                        if (StringUtils.isEmpty(end)) {
                            mapping.put(shardIndex, ImmutablePair.of(Long.parseLong(open), Long.MAX_VALUE));
                        } else {
                            mapping.put(shardIndex, ImmutablePair.of(Long.parseLong(open), Long.parseLong(end)));
                        }
                    } else {
                        if (StringUtils.isEmpty(key)) continue;
                        mapping.put(shardIndex, ImmutablePair.of(Long.parseLong(key), Long.MAX_VALUE));
                    }
                } catch (Exception e) {
                    throw e;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                fin.close();
            } catch (Exception e2) {
            }
        }
    }

    public String getRangeDefinitionFile() {
        return rangeDefinitionFile;
    }

    public void setRangeDefinitionFile(String rangeDefinitionFile) {
        this.rangeDefinitionFile = rangeDefinitionFile;
    }

    public Map<Integer, ImmutablePair<Long, Long>> getMapping() {
        return this.mapping;
    }

    public static void main(String[] args) {
        PartitionByRange f = new PartitionByRange("");
        f.setRangeDefinitionFile("conf/PartitionByRange.conf");
        f.init();
        for (Map.Entry<Integer, ImmutablePair<Long, Long>> entry : f.getMapping().entrySet()) {
            System.out.println(entry.getKey() + "/" + entry.getValue());
        }
    }
}


