using System;

namespace BrillPower.Ducklinq.Expressions;

public class ArithmeticExpression : OperatorExpression<ArithmeticOperator>
{
    internal ArithmeticExpression(DuckDbExpression left, DuckDbExpression right, ArithmeticOperator @operator) : base(left, right, @operator)
    {
    }

    public override Type Type => Left.Type; // TODO: improve

    public override string ToString()
    {
        return Operator switch
        {
            ArithmeticOperator.Add => $"{Left}+{Right}",
            ArithmeticOperator.Subtract => $"{Left}-{Right}",
            ArithmeticOperator.Multiply => $"{Left}*{Right}",
            ArithmeticOperator.Divide => $"{Left}/{Right}",
            ArithmeticOperator.Power => $"POWER({Left}, {Right})",
            _ => throw new NotSupportedException($"Operator {Operator} is not supported."),
        };
    }
}
