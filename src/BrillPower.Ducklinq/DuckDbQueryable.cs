using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace BrillPower.Ducklinq;

public class DuckDbQueryable<T> : IQueryable<T>, IOrderedQueryable<T>
{
    // https://weblogs.asp.net/dixin/understanding-linq-to-sql-10-implementing-linq-to-sql-provider
    internal DuckDbQueryable(DuckDbContext context)
    {
        Provider = new DuckDbQueryProvider(context);
        Expression = Expression.Constant(this);
    }

    internal DuckDbQueryable(DuckDbQueryProvider provider, Expression expression)
    {
        Provider = provider;
        Expression = expression;
    }

    public Type ElementType => typeof(T);

    public Expression Expression { get; }

    internal string? TableName { get; }

    internal DuckDbQueryProvider Provider { get; }

    IQueryProvider IQueryable.Provider => Provider;

    public IEnumerator<T> GetEnumerator()
    {
        return Provider.Execute<IEnumerable<T>>(Expression).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}
