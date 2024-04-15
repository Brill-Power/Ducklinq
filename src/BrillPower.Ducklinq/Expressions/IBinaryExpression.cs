namespace BrillPower.Ducklinq.Expressions;

public interface IBinaryExpression
{
    DuckDbExpression Left { get; }
    DuckDbExpression Right { get; }
}