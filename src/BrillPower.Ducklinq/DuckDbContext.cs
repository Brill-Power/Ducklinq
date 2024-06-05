using System;
using System.Collections.Generic;
using BrillPower.Ducklinq.Expressions;
using Dapper;
using DuckDB.NET.Data;

namespace BrillPower.Ducklinq;

public class DuckDbContext
{
    private readonly Func<DuckDBConnection> _connectionFactory;

    public DuckDbContext(string connectionString)
    {
        _connectionFactory = () =>
        {
            DuckDBConnection connection = new DuckDBConnection(connectionString);
            connection.Open();
            return connection;
        };
    }

    public DuckDbQueryable<T> Get<T>()
    {
        return new DuckDbQueryable<T>(this);
    }

    public virtual IEnumerable<T> Execute<T>(string query, IReadOnlyCollection<ParameterReferenceExpression> parameters)
    {
        using (DuckDBConnection connection = _connectionFactory())
        {
            DynamicParameters dynamicParameters = new DynamicParameters();
            foreach (ParameterReferenceExpression parameterReference in parameters)
            {
                dynamicParameters.Add(parameterReference.Name, parameterReference.Value);
            }
            return connection.Query<T>(query, dynamicParameters);
        }
    }
}
