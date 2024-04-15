using System;

namespace BrillPower.Ducklinq.Expressions;

public class IntervalExpression : DuckDbExpression
{
    public IntervalExpression(TimeSpan interval)
    {
        Interval = interval;
    }

    public TimeSpan Interval { get; }

    public override string ToString() => $"'{Interval}'::INTERVAL";
}