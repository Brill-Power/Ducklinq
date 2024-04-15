using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace BrillPower.Ducklinq.Expressions;

public class CallExpression : DuckDbExpression, IEquatable<CallExpression>
{
    internal CallExpression(string function, params DuckDbExpression[] arguments) : this(function, (IEnumerable<DuckDbExpression>)arguments)
    {
    }

    internal CallExpression(string function, IEnumerable<DuckDbExpression> arguments)
    {
        Function = function;
        Arguments = arguments.ToImmutableArray();
    }

    public string Function { get; }
    public IReadOnlyList<DuckDbExpression> Arguments { get; }
    public override Type Type => Arguments.Count > 0 ? Arguments[0].Type : typeof(DBNull); // this is crap

    public bool Equals(CallExpression? other)
    {
        return other is not null && Function.Equals(other.Function) && Arguments.SequenceEqual(other.Arguments);
    }

    public override bool Equals(object? obj) => obj is CallExpression that && Equals(that);

    public override int GetHashCode() => HashCode.Combine(Function.GetHashCode(), Arguments.Count.GetHashCode());

    public override string ToString() => $"{Function}({String.Join(", ", Arguments)})";
}