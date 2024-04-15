using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace BrillPower.Ducklinq.Expressions;

public class CaseExpression : DuckDbExpression
{
    public CaseExpression(DuckDbExpression? fallback, params (DuckDbExpression, DuckDbExpression)[] blocks) : this(fallback, (IEnumerable<(DuckDbExpression, DuckDbExpression)>)blocks)
    {
    }

    public CaseExpression(DuckDbExpression? fallback, IEnumerable<(DuckDbExpression, DuckDbExpression)> blocks)
    {
        Fallback = fallback;
        Blocks = blocks.ToImmutableArray();
    }

    public IReadOnlyCollection<(DuckDbExpression, DuckDbExpression)> Blocks { get; }
    public DuckDbExpression? Fallback { get; }

    public override string ToString()
    {
        return $"CASE{String.Join(String.Empty, Blocks.Select(b => $" WHEN {b.Item1} THEN {b.Item2}"))}{(Fallback is not null ? $" ELSE {Fallback}" : String.Empty)} END";
    }
}