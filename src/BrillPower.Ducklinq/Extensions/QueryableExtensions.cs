using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using BrillPower.Ducklinq.Expressions;
using BrillPower.Ducklinq.Schema;

namespace BrillPower.Ducklinq.Extensions;

public static class QueryableExtensions
{
    public static IQueryable<T> AsParquet<T>(this DuckDbQueryable<T> self)
    {
        string fileName = $"'{Table.GetTableName(self.ElementType)}.parquet'";
        return self.Provider.CreateQuery<T>(new SelectExpression(typeof(T), null, fileName, null));
    }

    public static IQueryable<T> AsPartitioned<T, TPartition>(this DuckDbQueryable<T> self, Expression<Func<T, TPartition>> partitionKeySelector, string path = "")
    {
        string tableName = Table.GetTableName(self.ElementType);
        string partitionPath = GetPath(partitionKeySelector);
        string parquetPath = Path.Combine(path, $"{tableName}/{partitionPath}/*.parquet");
        return self.Provider.CreateQuery<T>(new SelectExpression(typeof(T), null, $"read_parquet('{parquetPath}', hive_partitioning=1)", partitionKeySelector));
    }

    public static IQueryable<T> Cumulative<T>(this IQueryable<T> self)
    {
        return self.Provider.CreateQuery<T>(
            Expression.Call(
                new Func<IQueryable<T>, IQueryable<T>>(Cumulative).Method,
                self.Expression
            )
        );
    }

    public static IQueryable<TResult> AsOfJoin<TLeft, TRight, TKey, TResult>(this IQueryable<TLeft> self,
        IQueryable<TRight> right,
        Expression<Func<TLeft, DateTime>> leftTimestampSelector,
        Expression<Func<TRight, DateTime>> rightTimestampSelector,
        Expression<Func<TLeft, TKey>> leftKeySelector,
        Expression<Func<TRight, TKey>> rightKeySelector,
        Expression<Func<TLeft, TRight, TResult>> projector)
    {
        return self.Provider.CreateQuery<TResult>(
            Expression.Call(
                new Func<IQueryable<TLeft>, IQueryable<TRight>, Expression<Func<TLeft, DateTime>>, Expression<Func<TRight, DateTime>>, Expression<Func<TLeft, TKey>>, Expression<Func<TRight, TKey>>, Expression<Func<TLeft, TRight, TResult>>, IQueryable<TResult>>(AsOfJoin).Method,
                self.Expression,
                right.Expression,
                Expression.Quote(leftTimestampSelector),
                Expression.Quote(rightTimestampSelector),
                Expression.Quote(leftKeySelector),
                Expression.Quote(rightKeySelector),
                Expression.Quote(projector)
            )
        );
    }

    private static string GetPath(LambdaExpression partitionBy)
    {
        MemberFinder finder = new MemberFinder();
        finder.Visit(partitionBy);
        return String.Join("/", Enumerable.Range(0, finder.Properties.Count).Select(_ => "*"));
    }

    private class MemberFinder : ExpressionVisitor
    {
        public List<string> Properties { get; } = new List<string>();

        protected override Expression VisitNew(NewExpression node)
        {
            if (node.Arguments.Count > 0 && node.Members is not null)
            {
                for (int i = 0; i < node.Arguments.Count; i++)
                {
                    MemberInfo member = node.Members[i];
                    if (member is PropertyInfo property)
                    {
                        Properties.Add(property.Name);
                    }
                }
            }
            return node;
        }
    }
}
