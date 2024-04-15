using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace BrillPower.Ducklinq.Expressions;

public class WindowExpression : DuckDbExpression
{
    public WindowExpression(IEnumerable<DuckDbExpression> partitions, IEnumerable<OrderExpression> orderings, bool cumulative)
    {
        Partitions = partitions.ToImmutableArray();
        Orderings = orderings.ToImmutableArray();
        IsCumulative = cumulative;
    }

    public IReadOnlyCollection<DuckDbExpression> Partitions { get; }
    public IReadOnlyCollection<OrderExpression> Orderings { get; }
    public bool IsCumulative { get; }

    public override string ToString() => $"({(Partitions.Count > 0 ? $"PARTITION BY {String.Join(", ", Partitions)}" : String.Empty)}{(Orderings.Count > 0 ? $" ORDER BY {String.Join(", ", Orderings)}" : String.Empty)}{(IsCumulative ? " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" : String.Empty)})";
}