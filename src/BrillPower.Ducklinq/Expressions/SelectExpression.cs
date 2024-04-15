using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using BrillPower.Ducklinq.Schema;

namespace BrillPower.Ducklinq.Expressions;

public class SelectExpression : DuckDbExpression
{
    internal SelectExpression(Type elementType) : this(elementType, null, Table.GetTableName(elementType), null)
    {
    }

    internal SelectExpression(Type elementType, string? alias, string tableName, LambdaExpression? partitionKeySelector) : this(elementType, alias, new LiteralExpression(tableName, typeof(string)), null, [])
    {
        PartitionKeySelector = partitionKeySelector;
    }

    internal SelectExpression(Type elementType, string? alias, DuckDbExpression? source, LogicalExpression? predicate, params DuckDbExpression[] projections) : this(elementType, alias, source, predicate, projections, [], [], null, [])
    {
    }

    internal SelectExpression(Type elementType, string? alias, DuckDbExpression? source, LogicalExpression? predicate, IEnumerable<DuckDbExpression> projections, IEnumerable<DuckDbExpression> groupings, IEnumerable<AsExpression> windows, JoinClause? join, IEnumerable<OrderExpression> orderings) : this(elementType, alias, source, predicate, projections, groupings, windows, join, orderings, null)
    {
    }

    internal SelectExpression(Type elementType, string? alias, DuckDbExpression? source, LogicalExpression? predicate, IEnumerable<DuckDbExpression> projections, IEnumerable<DuckDbExpression> groupings, IEnumerable<AsExpression> windows, JoinClause? join, IEnumerable<OrderExpression> orderings, LambdaExpression? partitionKeySelector)
    {
        ElementType = elementType;
        Alias = alias;
        Source = source;
        Predicate = predicate;
        Projections = projections.ToImmutableArray();
        Groupings = groupings.ToImmutableArray();
        Windows = windows.ToImmutableArray();
        Orderings = orderings.ToImmutableArray();
        Join = join;
        Type = typeof(IQueryable<>).MakeGenericType(elementType);
        PartitionKeySelector = partitionKeySelector;
    }

    public DuckDbExpression? Source { get; }
    public LogicalExpression? Predicate { get; }
    public IReadOnlyCollection<DuckDbExpression> Projections { get; }
    public IReadOnlyCollection<DuckDbExpression> Groupings { get; }
    public IReadOnlyCollection<AsExpression> Windows { get; }
    public IReadOnlyCollection<OrderExpression> Orderings { get; }
    public JoinClause? Join { get; }
    public Type ElementType { get; }
    public string? Alias { get; }
    internal LambdaExpression? PartitionKeySelector { get; }

    public override Type Type { get; }

    public override string ToString()
    {
        StringBuilder sqlBuilder = new("SELECT ");
        if (Projections.Count > 0)
        {
            sqlBuilder.Append(String.Join(", ", Projections));
        }
        else
        {
            sqlBuilder.Append("*");
        }

        if (Source is SelectExpression)
        {
            sqlBuilder.Append($" FROM ({Source})");
        }
        else
        {
            sqlBuilder.Append($" FROM {Source}");
        }

        if (Alias is not null)
        {
            sqlBuilder.Append($" AS {Alias}");
        }
        if (Join is not null)
        {
            sqlBuilder.Append($" {Join}");
        }
        if (Predicate is not null)
        {
            sqlBuilder.Append($" WHERE {Predicate}");
        }
        if (Groupings.Count > 0)
        {
            sqlBuilder.Append($" GROUP BY {String.Join(", ", Groupings)}");
        }
        if (Windows.Count > 0)
        {
            sqlBuilder.Append($" WINDOW {String.Join(", ", Windows.Select(w => $"{w.Alias} AS {w.Expression}"))}");
        }
        if (Orderings.Count > 0)
        {
            sqlBuilder.Append($" ORDER BY {String.Join(", ", Orderings)}");
        }

        return sqlBuilder.ToString();
    }
}
