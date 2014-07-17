package com.alibaba.cobar.route.function;

import com.alibaba.cobar.config.model.rule.RuleAlgorithm;
import com.alibaba.cobar.parser.ast.expression.Expression;
import com.alibaba.cobar.parser.ast.expression.primary.function.FunctionExpression;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * rule for range based on long number
 *
 * just borrow ideas from @{com.alibaba.cobar.route.function.PartitionByFileMap}
 *
 * @author yunshi@wacai.com
 */
public class PartitionByRange extends FunctionExpression implements RuleAlgorithm {


    private String rangeDefinitionFile;

    private Map<Range, Integer> mapping;

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


        for (Map.Entry<Range, Integer> entry : mapping.entrySet()) {
            if (entry.getKey().in(key.longValue())) {
                rst[0] = entry.getValue();
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
            mapping = new HashMap<Range, Integer>();
            for (String line; (line = in.readLine()) != null; ) {
                line = line.trim();
                if (line.startsWith("#") || line.startsWith("//"))
                    continue;
                int ind = line.indexOf('=');
                if (ind < 0)
                    continue;
                try {
                    String range = line.substring(0, ind).trim();
                    int shardIndex = Integer.parseInt(line.substring(ind + 1).trim());
                    int rangeSep = range.indexOf(",");
                    if (rangeSep > 0) {
                        long low = Long.parseLong(range.substring(0, rangeSep));
                        long high = Long.MAX_VALUE;
                        if (rangeSep + 1 < range.length()) {
                            high = Long.parseLong(range.substring(rangeSep + 1).trim());
                        }
                        mapping.put(new Range(low, high), shardIndex);
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

    public Map<Range, Integer> getMapping() {
        return this.mapping;
    }

    public static class Range {
        private Long low;
        private Long high;

        public Range(Long lowMark, Long highMark) {
            low = lowMark;
            high = highMark;
        }


        public Long getLow() {
            return low;
        }

        public void setLow(Long low) {
            this.low = low;
        }

        public Long getHigh() {
            return high;
        }

        public void setHigh(Long high) {
            this.high = high;
        }

        public boolean in(Long key) {
            if (key >= low && key < high) return true;
            return false;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Range range = (Range) o;

            if (!high.equals(range.high)) return false;
            if (!low.equals(range.low)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = low.hashCode();
            result = 31 * result + high.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Range{" +
                    "low=" + low +
                    ", high=" + high +
                    '}';
        }
    }

    public static void main(String[] args) {
        PartitionByRange f = new PartitionByRange("");
        f.setRangeDefinitionFile("/Users/fuqiangwang/Downloads/aa.txt");
        f.init();
        for (Map.Entry<Range, Integer> entry : f.getMapping().entrySet()) {
            System.out.println(entry.getKey() + "/" + entry.getValue());
        }
    }
}


