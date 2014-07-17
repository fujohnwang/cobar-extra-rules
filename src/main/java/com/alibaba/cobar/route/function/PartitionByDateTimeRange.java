package com.alibaba.cobar.route.function;

import com.alibaba.cobar.config.model.rule.RuleAlgorithm;
import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.function.FunctionExpression;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.*;
import java.sql.Date;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * partition by range with datetime data type in mysql or other database providers.
 * <p/>
 * Created by @yunshi@wacai.com on 7/16/14.
 */
public class PartitionByDateTimeRange extends FunctionExpression implements RuleAlgorithm {
    private final String datePattern = "yyyy-MM-dd HH:mm:ss";

    private String rangeDefinitionFile;

    private Map<Integer, ImmutablePair<Long, Long>> mapping;  // integer is better for key, since we will scan the whole map, so this is acceptable.

    public PartitionByDateTimeRange(String functionName) {
        super(functionName, null);
    }

    public PartitionByDateTimeRange(String functionName, List<Expression> arguments) {
        super(functionName, arguments);
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

    @Override
    public RuleAlgorithm constructMe(Object... objects) {
        List<Expression> args = new ArrayList<Expression>(objects.length);
        for (Object obj : objects) {
            args.add((Expression) obj);
        }
        PartitionByDateTimeRange func = new PartitionByDateTimeRange(functionName, args);
        func.rangeDefinitionFile = rangeDefinitionFile;
        return func;
    }


    @Override
    public void init() {
        initialize();
    }

    /**
     * rule format like:
     * <pre>
     *    1700-09-12 23:34:01,1800-09-12 23:59:59=0
     *    1800-09-12 23:59:59,=1
     * </pre>
     * <p/>
     * the last range is open, so no stake is ok after comma.
     * <p/>
     * the range is inclusive at the beginning and exclusive at the ending.
     */
    @Override
    public void initialize() {


        InputStream fin = null;
        try {
            fin = new FileInputStream(new File(rangeDefinitionFile));
            BufferedReader in = new BufferedReader(new InputStreamReader(fin));
            mapping = new HashMap<Integer, ImmutablePair<Long, Long>>();

            for (String line; (line = in.readLine()) != null; ) {
                line = line.trim();

                if (line.startsWith("#") || line.startsWith("//") || !StringUtils.contains(line, "="))
                    continue;

                String key = StringUtils.trimToEmpty(StringUtils.substringBefore(line, "="));
                int shardIndex = Integer.parseInt(StringUtils.trimToEmpty(StringUtils.substringAfter(line, "=")));

                if (StringUtils.contains(key, ",")) {
                    String open = StringUtils.trimToEmpty(StringUtils.substringBefore(key, ","));
                    String end = StringUtils.trimToEmpty(StringUtils.substringAfter(key, ","));
                    if (StringUtils.isEmpty(StringUtils.trimToEmpty(end))) {
                        mapping.put(shardIndex, ImmutablePair.of(DateUtils.parseDate(open, datePattern).getTime(), Long.MAX_VALUE));
                    } else {
                        mapping.put(shardIndex, ImmutablePair.of(DateUtils.parseDate(open, datePattern).getTime(), DateUtils.parseDate(end, datePattern).getTime()));
                    }
                } else {
                    if (StringUtils.isNotEmpty(key)) {
                        mapping.put(shardIndex, ImmutablePair.of(DateUtils.parseDate(key, datePattern).getTime(), Long.MAX_VALUE));
                    }
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

    @Override
    public Object evaluationInternal(Map<? extends Object, ? extends Object> parameters) {
        return calculate(parameters)[0];
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
        String key;
        if (arg instanceof String) {
            key = (String) arg;
        } else {
            throw new IllegalArgumentException("unsupported data type for partition key: " + arg.getClass());
        }

        try {
            Long datetime = DateUtils.parseDate(key, datePattern).getTime();
            for (Map.Entry<Integer, ImmutablePair<Long, Long>> e : mapping.entrySet()) {
                ImmutablePair<Long, Long> pair = e.getValue();
                if (datetime >= pair.getLeft() && datetime < pair.getRight()) {
                    rst[0] = e.getKey();
                    break;
                }
            }
        } catch (ParseException e) {
            throw new IllegalArgumentException("parse failed with argument: " + key, e);
        }
        return rst;
    }

    public void setRangeDefinitionFile(String rangeDefinitionFile) {
        this.rangeDefinitionFile = rangeDefinitionFile;
    }

    public Map<Integer, ImmutablePair<Long, Long>> getMapping() {
        return mapping;
    }

    public static void main(String[] args) {
        PartitionByDateTimeRange f = new PartitionByDateTimeRange("");
        f.setRangeDefinitionFile("conf/PartitionByDateTimeRange.conf");
        f.init();

        String pattern = "yyyy-MM-dd HH:mm:ss";
        for (Map.Entry<Integer, ImmutablePair<Long, Long>> entry : f.getMapping().entrySet()) {
            System.out.println(entry.getKey() + " / " + DateFormatUtils.format(new Date(entry.getValue().getLeft()), pattern) + "," + DateFormatUtils.format(new Date(entry.getValue().getRight()), pattern));
        }

    }
}

