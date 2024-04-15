using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using BrillPower.Ducklinq.Expressions;

namespace BrillPower.Ducklinq;

public class DuckDbQueryProvider : IQueryProvider
{
    private readonly DuckDbContext _context;

    public DuckDbQueryProvider(DuckDbContext context)
    {
        _context = context;
    }

    public IQueryable CreateQuery(Expression expression)
    {
        throw new NotImplementedException();
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        return new DuckDbQueryable<TElement>(this, expression);
    }

    public object? Execute(Expression expression)
    {
        Type? elementType = expression.Type
            .GetInterfaces()
            .Concat([expression.Type])
            .Where(i => i.IsGenericType && typeof(IEnumerable<>) == i.GetGenericTypeDefinition())
            .FirstOrDefault()?
            .GetGenericArguments()[0];
        if (elementType is null)
        {
            throw new NotSupportedException($"Scalar results are not currently supported.");
        }

        if (Activator.CreateInstance(typeof(Executor<>).MakeGenericType(elementType)) is Executor executor)
        {
            return executor.Execute(_context, expression);
        }

        throw new InvalidOperationException($"An unknown error has occured.");
    }

    public TResult Execute<TResult>(Expression expression)
    {
        object? result = Execute(expression);
        if (result is not null)
        {
            return (TResult)result;
        }
        return default!;
    }

    private abstract class Executor
    {
        public abstract object? Execute(DuckDbContext context, Expression expression);
    }

    private class Executor<T> : Executor
    {
        public override object? Execute(DuckDbContext context, Expression expression)
        {
            QueryTranslator queryTranslator = new QueryTranslator();
            string query = queryTranslator.Translate(expression);
            return context.Execute<T>(query, queryTranslator.ParameterReferences);
        }
    }
}
