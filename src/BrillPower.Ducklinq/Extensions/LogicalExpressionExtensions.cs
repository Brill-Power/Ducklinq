using BrillPower.Ducklinq.Expressions;

namespace BrillPower.Ducklinq.Extensions;

public static class LogicalExpressionExtensions
{
    public static LogicalExpression AndOthers(this LogicalExpression original, params LogicalExpression[] others)
    {
        return original.CombineAs(LogicalOperator.AndAlso, others);
    }

    public static LogicalExpression OrOthers(this LogicalExpression original, params LogicalExpression[] others)
    {
        LogicalExpression result = original.CombineAs(LogicalOperator.OrElse, others);
        return new LogicalExpression(result.Left, result.Right, LogicalOperator.Or); // wrap the last pair with brackets
    }

    private static LogicalExpression CombineAs(this LogicalExpression original, LogicalOperator @operator, params LogicalExpression[] others)
    {
        LogicalExpression result = original;
        foreach (LogicalExpression another in others)
        {
            result = new LogicalExpression(result, another, @operator);
        }
        return result;
    }
}