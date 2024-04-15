using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace BrillPower.Ducklinq.Expressions;

public class JoinClause
{
    public JoinClause(SelectExpression table, IEnumerable<LogicalExpression> keyComparisons, bool asOf)
    {
        if (table.Source is LiteralExpression && table.Predicate is null)
        {
            Table = table.Source;
            Alias = table.Alias ?? throw new ArgumentNullException($"Table {table.Source} has no alias.");
        }
        else if (table.Source is SelectExpression { Source: LiteralExpression } selectExpression)
        {
            Table = selectExpression.Source;
            Alias = selectExpression.Alias ?? throw new ArgumentNullException($"Table {selectExpression.Source} has no alias.");
        }
        else
        {
            Table = table;
            Alias = "y"; // TODO
        }
        KeyComparisons = keyComparisons.ToImmutableArray();
        AsOf = asOf;
    }

    public JoinClause(SelectExpression table, params (DuckDbExpression leftKey, DuckDbExpression rightKey)[] keyPairs) : this(table, keyPairs.Select(pair => new LogicalExpression(pair.leftKey, pair.rightKey, LogicalOperator.Equal)), false)
    {
    }

    public DuckDbExpression Table { get; }
    public string Alias { get; }
    public bool AsOf { get; }
    public IReadOnlyCollection<DuckDbExpression> KeyComparisons { get; }

    public override string ToString()
    {
        return $"{(AsOf ? "ASOF " : String.Empty)}JOIN {Table} AS {Alias} ON {String.Join(" AND ", KeyComparisons)}";
    }
}