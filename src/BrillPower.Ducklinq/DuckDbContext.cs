using System;
using System.Collections.Generic;
using BrillPower.Ducklinq.Expressions;
using Dapper;
using DuckDB.NET.Data;

namespace BrillPower.Ducklinq;

public class DuckDbContext : IDisposable
{
    private readonly DuckDBConnection _connection;

    public DuckDbContext(DuckDBConnection connection)
    {
        _connection = connection;
    }

    public DuckDbContext(string connectionString)
    {
        _connection = new DuckDBConnection(connectionString);
        _connection.Open();
    }

    public DuckDbQueryable<T> Get<T>()
    {
        return new DuckDbQueryable<T>(this);
    }

    public virtual IEnumerable<T> Execute<T>(string query, IReadOnlyCollection<ParameterReferenceExpression> parameters)
    {
        DynamicParameters dynamicParameters = new DynamicParameters();
        foreach (ParameterReferenceExpression parameterReference in parameters)
        {
            dynamicParameters.Add(parameterReference.Name, parameterReference.Value);
        }
        return _connection.Query<T>(query, dynamicParameters);
    }

    public void Dispose()
    {
        _connection.Dispose();
    }
}
