using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using BrillPower.Ducklinq.Expressions.Fixup;
using BrillPower.Ducklinq.Extensions;
using BrillPower.Ducklinq.Schema;

namespace BrillPower.Ducklinq.Expressions;

public class QueryTranslator : ExpressionVisitor
{
    private readonly List<ParameterReferenceExpression> _parameterReferences = new List<ParameterReferenceExpression>();
    private readonly Stack<DuckDbExpression> _groupingKeys = new Stack<DuckDbExpression>();
    private readonly Stack<AsExpression> _windows = new Stack<AsExpression>();
    private readonly Stack<AsExpression> _allWindows = new Stack<AsExpression>();
    private readonly Stack<DuckDbExpression> _cumulatives = new Stack<DuckDbExpression>();
    private readonly Stack<DuckDbExpression> _selectManys = new Stack<DuckDbExpression>();
    private int _tableAliasIndex = 0;
    private int _windowAliasIndex = 0;
    private readonly ConcurrentDictionary<Type, Stack<string>> _aliasesByElementType = new ConcurrentDictionary<Type, Stack<string>>();

    public IReadOnlyCollection<ParameterReferenceExpression> ParameterReferences => _parameterReferences;

    public string Translate(Expression expression)
    {
        DuckDbExpression visited = (DuckDbExpression)Visit(expression);
        visited = new SelectReducer().Visit(visited);
        visited = new PartitionedWhere().Visit(visited);
        visited = new Inbetweener().Visit(visited);
        return visited.ToString();
    }

    protected override Expression VisitConstant(ConstantExpression node)
    {
        // see if it is a table
        if (node.Type.IsGenericType && node.Value is IQueryable queryable)
        {
            string tableAlias = GetTableAlias();
            _aliasesByElementType.GetOrAdd(queryable.ElementType, _ => new Stack<string>()).Push(tableAlias);
            return new SelectExpression(queryable.ElementType, tableAlias, Table.GetTableName(queryable.ElementType), null);
        }
        else if (node.Type == typeof(TimeSpan) && node.Value is TimeSpan timeSpan)
        {
            return new IntervalExpression(timeSpan);
        }
        else
        {
            return GetParameterReference(node.Value, node.Type);
        }
    }

    protected override Expression VisitExtension(Expression node)
    {
        if (node is SelectExpression selectExpression && selectExpression.Source is LiteralExpression && selectExpression.Alias is null)
        {
            string tableAlias = GetTableAlias();
            _aliasesByElementType.GetOrAdd(selectExpression.ElementType, _ => new Stack<string>()).Push(tableAlias);
            return new SelectExpression(selectExpression.ElementType, tableAlias, selectExpression.Source, null, [], [], [], null, [], selectExpression.PartitionKeySelector);
        }
        return node;
    }

    protected override Expression VisitParameter(ParameterExpression node)
    {
        return new EmptyExpression();
    }

    private DuckDbExpression VisitNewOrMemberInit(Type elementType, int count, IEnumerable<(Expression, MemberInfo)> bindings)
    {
        Stack<string> stack = new Stack<string>();
        string? tableAlias = null;
        if (_aliasesByElementType.TryAdd(elementType, stack))
        {
            tableAlias = GetTableAlias();
            stack.Push(tableAlias);
        }
        DuckDbExpression[] projections = new DuckDbExpression[count];
        int i = 0;
        foreach ((Expression argument, MemberInfo member) in bindings)
        {
            DuckDbExpression expression = VisitChecked(argument);
            if (expression is ColumnReferenceExpression columnReference &&
                columnReference.Name == member.Name)
            {
                // check references to GroupBy columns
                if (_groupingKeys.TryPeek(out DuckDbExpression? key) &&
                    (columnReference.Name == nameof(IGrouping<string, string>.Key) ||
                    argument is MemberExpression
                    {
                        Expression: MemberExpression { Member.Name: nameof(IGrouping<string, string>.Key) }
                    }))
                {
                    if (key is ColumnReferenceExpression keyRef && String.Equals(keyRef.Name, columnReference.Name))
                    {
                        // cleanliness: don't re-alias column with same name
                        projections[i] = key;
                    }
                    else
                    {
                        // alias as necessary
                        projections[i] = new AsExpression(key, columnReference.Name);
                    }
                    _groupingKeys.Pop();
                }
                else
                {
                    // just include column
                    projections[i] = columnReference;
                }
            }
            else
            {
                if (_allWindows.TryPeek(out AsExpression? window))
                {
                    expression = new OverExpression(expression, window.Alias);
                }
                expression = new AsExpression(expression, member.Name);
                projections[i] = expression;
            }
            i++;
        }
        return new SelectExpression(elementType, tableAlias, null, null, projections);
    }

    protected override Expression VisitMemberInit(MemberInitExpression node)
    {
        return VisitNewOrMemberInit(node.Type, node.Bindings.Count, node.Bindings.Cast<MemberAssignment>().Select(ma => (ma.Expression, ma.Member)));
    }

    protected override Expression VisitNew(NewExpression node)
    {
        if (node.Arguments.Count > 0 && node.Members is not null)
        {
            return VisitNewOrMemberInit(node.Type, node.Arguments.Count, node.Arguments.Select((a, i) => (a, node.Members[i])));
        }
        return base.VisitNew(node);
    }

    public DuckDbExpression VisitChecked(Expression node)
    {
        if (Visit(node) is DuckDbExpression duck)
        {
            return duck;
        }
        throw new NotSupportedException($"Encountered a node of type {node.NodeType} which was not translated.");
    }

    protected override Expression VisitBinary(BinaryExpression node)
    {
        DuckDbExpression left = VisitChecked(node.Left);
        DuckDbExpression right = VisitChecked(node.Right);
        return node.NodeType switch
        {
            ExpressionType.Add => new ArithmeticExpression(left, right, ArithmeticOperator.Add),
            ExpressionType.Subtract => new ArithmeticExpression(left, right, ArithmeticOperator.Subtract),
            ExpressionType.Multiply => new ArithmeticExpression(left, right, ArithmeticOperator.Multiply),
            ExpressionType.Divide => new ArithmeticExpression(left, right, ArithmeticOperator.Divide),
            ExpressionType.Power => new ArithmeticExpression(left, right, ArithmeticOperator.Power),
            ExpressionType.Equal => new LogicalExpression(left, right, LogicalOperator.Equal),
            ExpressionType.NotEqual => new LogicalExpression(left, right, LogicalOperator.NotEqual),
            ExpressionType.GreaterThan => new LogicalExpression(left, right, LogicalOperator.GreaterThan),
            ExpressionType.GreaterThanOrEqual => new LogicalExpression(left, right, LogicalOperator.GreaterThanOrEqual),
            ExpressionType.LessThan => new LogicalExpression(left, right, LogicalOperator.LessThan),
            ExpressionType.LessThanOrEqual => new LogicalExpression(left, right, LogicalOperator.LessThanOrEqual),
            ExpressionType.AndAlso => new LogicalExpression(left, right, LogicalOperator.AndAlso),
            ExpressionType.OrElse => new LogicalExpression(left, right, LogicalOperator.OrElse),
            ExpressionType.Coalesce => new CoalesceExpression(left, right),
            _ => throw new NotSupportedException($"{node.NodeType} expression is not supported.")
        };
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        switch (node.Method.Name)
        {
            case nameof(Enumerable.Where):
                return VisitWhere(node);
            case nameof(Enumerable.Select):
                return VisitSelect(node);
            case nameof(Enumerable.SelectMany):
                return VisitSelectMany(node);
            case nameof(Enumerable.GroupBy):
                return VisitGroupBy(node);
            case nameof(Enumerable.OrderBy):
            case nameof(Enumerable.OrderByDescending):
                return VisitOrderBy(node, ascending: node.Method.Name == nameof(Enumerable.OrderBy));
            case nameof(Enumerable.Join):
                return VisitJoin(node, asOf: false);
            case nameof(Enumerable.Union):
            case nameof(Enumerable.Concat):
                return VisitUnion(node, all: node.Method.Name == nameof(Enumerable.Concat));
            case nameof(Enumerable.Contains):
                return VisitContains(node);
            case nameof(Enumerable.Max):
            case nameof(Enumerable.Min):
            case nameof(Enumerable.Sum):
            case nameof(Enumerable.Average):
            case nameof(Enumerable.First):
            case nameof(Enumerable.Last):
                return VisitAggregation(node);
            case nameof(DateTimeExtensions.TimeBucket):
                return VisitTimeBucket(node);
            case nameof(QueryableExtensions.AsOfJoin):
                return VisitJoin(node, asOf: true);
            case nameof(QueryableExtensions.Cumulative):
                return VisitCumulative(node);
            case nameof(Math.Abs):
                return VisitMaths(node);
            default:
                if (node.Method.DeclaringType == typeof(TimeSpan))
                {
                    return EvaluateAndVisit(node);
                }
                break;
        }
        throw new NotSupportedException($"Method {node.Method.Name} is not supported.");
    }

    private Expression EvaluateAndVisit(Expression node)
    {
        LambdaExpression lambda = Expression.Lambda(node);
        Delegate function = lambda.Compile();
        return Visit(Expression.Constant(function.DynamicInvoke(null), node.Type));
    }

    private DuckDbExpression VisitMaths(MethodCallExpression node)
    {
        DuckDbExpression expression = VisitChecked(node.Arguments[1]);
        string function;
        switch (node.Method.Name)
        {
            case nameof(Math.Abs):
                function = "ABS";
                break;
            default:
                throw new NotSupportedException($"Method {node.Method.Name} is not supported.");
        }
        return new CallExpression(function, expression);
    }

    private DuckDbExpression VisitTimeBucket(MethodCallExpression node)
    {
        DuckDbExpression bucketedColumn = VisitChecked(node.Arguments[0]);
        DuckDbExpression interval = VisitChecked(node.Arguments[1]);
        if (interval is IntervalExpression)
        {
            return new CallExpression("time_bucket", interval, bucketedColumn);
        }
        else
        {
            throw new NotSupportedException("Unsupported interval specified in time bucket.");
        }
    }

    private DuckDbExpression VisitAggregation(MethodCallExpression node)
    {
        DuckDbExpression expression = VisitChecked(node.Arguments[1]);
        DuckDbExpression source = VisitChecked(node.Arguments[0]);
        TryPushWindow(source, false, out SelectExpression? sourceSelect);
        string function;
        switch (node.Method.Name)
        {
            case nameof(Enumerable.Max):
                function = "MAX";
                break;
            case nameof(Enumerable.Min):
                function = "MIN";
                break;
            case nameof(Enumerable.Average):
                function = "AVG";
                break;
            case nameof(Enumerable.Sum):
                function = "SUM";
                break;
            case nameof(Enumerable.Count):
                function = "COUNT";
                break;
            case nameof(Enumerable.First):
                function = "FIRST";
                break;
            case nameof(Enumerable.Last):
                function = "LAST";
                break;
            default:
                throw new NotSupportedException($"Method {node.Method.Name} is not supported.");
        }
        return new CallExpression(function, expression);
    }

    protected override Expression VisitConditional(ConditionalExpression node)
    {
        DuckDbExpression test = VisitChecked(node.Test);
        DuckDbExpression ifTrue = VisitChecked(node.IfTrue);
        DuckDbExpression ifFalse = VisitChecked(node.IfFalse);
        return new CaseExpression(ifFalse, (test, ifTrue));
    }

    protected override Expression VisitLambda<T>(Expression<T> node)
    {
        return VisitChecked(node.Body); // unquote
    }

    protected override Expression VisitUnary(UnaryExpression node)
    {
        DuckDbExpression operand = VisitChecked(node.Operand);
        return node.NodeType switch
        {
            ExpressionType.Quote => operand,
            ExpressionType.Convert => operand,
            _ => throw new NotSupportedException($"{node.NodeType} expression is not supported.")
        };
    }

    protected override Expression VisitMember(MemberExpression node)
    {
        if (node.Member is PropertyInfo property)
        {
            if (property.DeclaringType == typeof(DateTime) && node.Expression is not null)
            {
                DuckDbExpression datetime = VisitChecked(node.Expression);
                string part;
                switch (node.Member.Name)
                {
                    case nameof(DateTime.Day):
                        part = "day";
                        break;
                    case nameof(DateTime.Month):
                        part = "month";
                        break;
                    case nameof(DateTime.Year):
                        part = "year";
                        break;
                    case nameof(DateTime.Hour):
                        part = "hour";
                        break;
                    case nameof(DateTime.Minute):
                        part = "minute";
                        break;
                    case nameof(DateTime.Second):
                        part = "second";
                        break;
                    default:
                        throw new NotSupportedException($"DateTime property '{node.Member.Name}' is not currently supported.");
                }
                return new CallExpression("date_part",
                    new LiteralExpression($"'{part}'", typeof(string)),
                    datetime);
            }
            string? alias;
            if (!_aliasesByElementType.TryGetValue(node.Expression!.Type, out Stack<string>? aliases) ||
                !aliases.TryPeek(out alias))
            {
                alias = null;
            }
            return new ColumnReferenceExpression(node.Member.Name, alias, property.PropertyType);
        }
        else if (node.Member is FieldInfo)
        {
            // probably a captured local
            return EvaluateAndVisit(node);
        }
        throw new NotSupportedException($"Members of type {node.Member.MemberType} are not supported.");
    }

    private DuckDbExpression VisitContains(MethodCallExpression node)
    {
        // note order is reversed - y.Contains(x) becomes x IN y
        DuckDbExpression right = VisitChecked(node.Arguments[0]);
        DuckDbExpression left = VisitChecked(node.Arguments[1]);
        return new LogicalExpression(left, right, LogicalOperator.Contains);
    }

    private DuckDbExpression VisitUnion(MethodCallExpression node, bool all)
    {
        DuckDbExpression left = VisitChecked(node.Arguments[0]);
        DuckDbExpression right = VisitChecked(node.Arguments[1]);
        if (left is SelectExpression leftSelect && right is SelectExpression rightSelect)
        {
            return new UnionExpression(leftSelect, rightSelect, all);
        }
        else
        {
            throw new NotSupportedException($"Expected Select queries as arguments to both sides of a Union.");
        }
    }

    private DuckDbExpression VisitJoin(MethodCallExpression node, bool asOf)
    {
        DuckDbExpression left = VisitChecked(node.Arguments[0]);
        DuckDbExpression right = VisitChecked(node.Arguments[1]);
        int index = 2;
        List<LogicalExpression> keyComparisons = new List<LogicalExpression>();
        if (asOf)
        {
            DuckDbExpression leftTimestamp = VisitChecked(node.Arguments[2]);
            DuckDbExpression rightTimestamp = VisitChecked(node.Arguments[3]);
            keyComparisons.Add(new LogicalExpression(leftTimestamp, rightTimestamp, LogicalOperator.GreaterThanOrEqual));
            index = 4;
        }
        DuckDbExpression leftKey = VisitChecked(node.Arguments[index]);
        DuckDbExpression rightKey = VisitChecked(node.Arguments[index + 1]);

        if (leftKey is SelectExpression leftKeySelect &&
            rightKey is SelectExpression rightKeySelect)
        {
            if (leftKeySelect.Projections.Count != rightKeySelect.Projections.Count)
            {
                throw new NotSupportedException("Join keys must have the same number of columns.");
            }

            DuckDbExpression[] leftKeys = leftKeySelect.Projections.ToArray();
            DuckDbExpression[] rightKeys = rightKeySelect.Projections.ToArray();
            for (int i = 0; i < leftKeys.Length; i++)
            {
                keyComparisons.Add(new LogicalExpression(leftKeys[i], rightKeys[i], LogicalOperator.Equal));
            }
        }
        else
        {
            keyComparisons.Insert(Math.Max(keyComparisons.Count - 1, 0), new LogicalExpression(leftKey, rightKey, LogicalOperator.Equal));
        }
        DuckDbExpression projection = VisitChecked(node.Arguments[index + 2]);
        if (projection is SelectExpression selectProjection && selectProjection.Source is null && right is SelectExpression rightSelect)
        {
            JoinClause joinClause = new JoinClause(rightSelect, keyComparisons, asOf);
            LogicalExpression? predicate = rightSelect.Predicate;

            return new SelectExpression(node.Arguments[4].Type, null, left, predicate, selectProjection.Projections, [], [], joinClause, []);
        }
        throw new NotSupportedException($"Encountered a join that was not supported.");
    }

    private DuckDbExpression VisitGroupBy(MethodCallExpression node)
    {
        DuckDbExpression source = VisitChecked(node.Arguments[0]);

        DuckDbExpression key = VisitChecked(node.Arguments[1]);

        if (key is SelectExpression selectExpression)
        {
            foreach (DuckDbExpression projection in selectExpression.Projections)
            {
                if (projection is AsExpression asExpression)
                {
                    _groupingKeys.Push(asExpression.Expression);
                    continue;
                }
                _groupingKeys.Push(projection);
            }
        }
        else
        {
            _groupingKeys.Push(key); // push for use in later projection
        }

        return new SelectExpression(node.Method.GetGenericArguments()[0], null, source, null, [], _groupingKeys, [], null, []);
    }

    private DuckDbExpression VisitOrderBy(MethodCallExpression node, bool ascending)
    {
        DuckDbExpression source = VisitChecked(node.Arguments[0]);
        ColumnReferenceExpression column = (ColumnReferenceExpression)VisitChecked(node.Arguments[1]);
        return new SelectExpression(node.Method.GetGenericArguments()[0], null, source, null, [], [], [], null, [new OrderExpression(column, ascending)]);
    }

    private DuckDbExpression VisitCumulative(MethodCallExpression node)
    {
        DuckDbExpression source = VisitChecked(node.Arguments[0]);
        _cumulatives.Push(source);
        return source;
    }

    private DuckDbExpression VisitSelectMany(MethodCallExpression node)
    {
        DuckDbExpression source = VisitChecked(node.Arguments[0]);
        if (TryPushWindow(source, true, out SelectExpression? sourceSelect))
        {
            int windowCount = _allWindows.Count - 1;
            DuckDbExpression projection = VisitChecked(node.Arguments[1]);
            if (projection is SelectExpression projectionSelect)
            {
                List<AsExpression> windows = new List<AsExpression>();
                while (_allWindows.Count > windowCount)
                {
                    windows.Add(_allWindows.Pop());
                }
                _windows.Pop();
                return new SelectExpression(projectionSelect.ElementType, sourceSelect.Alias, sourceSelect.Source, sourceSelect.Predicate, projectionSelect.Projections, [], windows, null, []);
            }
            else
            {
                throw new NotSupportedException($"Unsupported SelectMany (expected to find a projection and did not find one).");
            }
        }
        throw new NotSupportedException($"SelectMany not supported except following a GroupBy.");
    }

    private DuckDbExpression VisitSelect(MethodCallExpression node)
    {
        DuckDbExpression source = VisitChecked(node.Arguments[0]);
        DuckDbExpression projection = VisitChecked(node.Arguments[1]);
        if (projection is SelectExpression selectProjection && selectProjection.Source is null)
        {
            string? tableAlias = GetCurrentAlias(node.Method.GetGenericArguments()[0]);
            return new SelectExpression(node.Method.GetGenericArguments()[1], tableAlias, source, null, selectProjection.Projections, [], [], null, []);
        }
        throw new NotSupportedException($"Encountered a select projection that was not supported.");
    }

    private DuckDbExpression VisitWhere(MethodCallExpression node)
    {
        DuckDbExpression source = VisitChecked(node.Arguments[0]);
        LogicalExpression predicate = (LogicalExpression)VisitChecked(node.Arguments[1]);
        string? tableAlias = GetCurrentAlias(node.Method.GetGenericArguments()[0]);
        return new SelectExpression(node.Method.GetGenericArguments()[0], tableAlias, source, predicate);
    }

    private string? GetCurrentAlias(Type type)
    {
        string? tableAlias = null;
        if (_aliasesByElementType.TryGetValue(type, out Stack<string>? stack))
        {
            stack.TryPeek(out tableAlias);
        }
        return tableAlias;
    }

    private string GetTableAlias()
    {
        return $"t{_tableAliasIndex++}";
    }

    private string GetWindowAlias()
    {
        return $"w{_windowAliasIndex++}";
    }

    private bool TryPushWindow(DuckDbExpression source, bool outer, [NotNullWhen(true)] out SelectExpression? sourceSelect)
    {
        source = new SelectReducer().Visit(source);
        if (source is SelectExpression ss && (ss.Groupings.Count > 0 || ss.Orderings.Count > 0))
        {
            sourceSelect = ss;
            // exciting, PARTITION BY...
            bool isCumulative = false;
            if (_cumulatives.TryPeek(out DuckDbExpression? cumulativeSource) && cumulativeSource.Type == source.Type)
            {
                _cumulatives.Pop();
                isCumulative = true;
            }
            IReadOnlyCollection<DuckDbExpression> groupings = sourceSelect.Groupings;
            IReadOnlyCollection<OrderExpression> orderings = sourceSelect.Orderings;
            if (!outer && _windows.Peek().Expression is WindowExpression outerWindow)
            {
                groupings = outerWindow.Partitions.Concat(groupings).ToList();
                orderings = outerWindow.Orderings.Concat(orderings).ToList();
            }
            WindowExpression window = new WindowExpression(groupings, orderings, isCumulative);
            string windowAlias = GetWindowAlias();
            AsExpression aliasedWindow = new AsExpression(window, windowAlias);
            _allWindows.Push(aliasedWindow);
            if (outer)
            {
                _windows.Push(aliasedWindow);
            }
            return true;
        }

        sourceSelect = default;
        return false;
    }

    private ParameterReferenceExpression GetParameterReference(object? value, Type type)
    {
        ParameterReferenceExpression parameterReference = new ParameterReferenceExpression($"p{_parameterReferences.Count}", value, type);
        _parameterReferences.Add(parameterReference);
        return parameterReference;
    }
}
