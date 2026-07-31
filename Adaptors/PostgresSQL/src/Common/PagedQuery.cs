// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using Npgsql;

namespace ArmoniK.Core.Adapters.PostgresSQL.Common;

/// <summary>
///   A query and the parameters it references.
/// </summary>
/// <param name="Sql">The query</param>
/// <param name="Parameters">Parameters referenced by <paramref name="Sql" /></param>
public readonly record struct Query(string                      Sql,
                                    Dictionary<string, object?> Parameters);

/// <summary>
///   Runs the two queries behind a paginated list: the total number of matches, and one page of rows.
/// </summary>
public static class PagedQuery
{
  /// <summary>
  ///   Count the matches and read the requested page of a single table, deriving both queries from
  ///   <paramref name="filter" /> and <paramref name="orderField" />:
  ///   <c>SELECT COUNT(*)</c> for the count and, for the page, <paramref name="selectList" /> ordered on
  ///   one column.
  /// </summary>
  /// <remarks>
  ///   Queries that don't have that shape - one counting distinct combinations of columns, say, or one
  ///   ordering on several fields - build their own and call the overload taking them directly.
  /// </remarks>
  /// <typeparam name="TEntity">Entity type the filter and order field apply to</typeparam>
  /// <typeparam name="TRow">Type each row maps to</typeparam>
  /// <param name="connectionProvider">Provider of pooled connections</param>
  /// <param name="table">Table to select from</param>
  /// <param name="filter">Filter both queries apply</param>
  /// <param name="orderField">Field the page is ordered on</param>
  /// <param name="ascOrder">Whether the page is ordered ascending</param>
  /// <param name="page">Zero-based index of the page to read</param>
  /// <param name="pageSize">Rows per page; when not positive no page is read and no rows are returned</param>
  /// <param name="mapRow">Maps the reader's current row to <typeparamref name="TRow" /></param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <param name="selectList">
  ///   Columns the page selects, <c>*</c> for every one. Pass <see cref="RowMapper.SelectList" /> to read
  ///   only what a projection needs, keeping it in step with the mapper handed to
  ///   <paramref name="mapRow" />.
  /// </param>
  /// <returns>The page's rows, and the total number of matches irrespective of paging</returns>
  public static async Task<(List<TRow> items, long totalCount)> ExecuteAsync<TEntity, TRow>(NpgsqlConnectionProvider           connectionProvider,
                                                                                            string                             table,
                                                                                            Expression<Func<TEntity, bool>>    filter,
                                                                                            Expression<Func<TEntity, object?>> orderField,
                                                                                            bool                               ascOrder,
                                                                                            int                                page,
                                                                                            int                                pageSize,
                                                                                            Func<NpgsqlDataReader, TRow>       mapRow,
                                                                                            CancellationToken                  cancellationToken,
                                                                                            string                             selectList = "*")
  {
    var (whereSql, parameters)     = ExpressionToSql<TEntity>.Translate(filter);
    var (orderColumn, orderParams) = ExpressionToSql<TEntity>.TranslateOrderBy(orderField);

    // Only the page references the ORDER BY parameters, so they are kept out of the count's set rather
    // than merged into it. The two namespaces are disjoint - filters are named @p<n> and order keys
    // @orderkey<n> - so nothing is overwritten either way round.
    foreach (var (key, value) in parameters)
    {
      orderParams[key] = value;
    }

    var orderDir = ascOrder
                     ? "ASC"
                     : "DESC";

    return await ExecuteAsync(connectionProvider,
                              new Query($"SELECT COUNT(*) FROM {table} WHERE {whereSql}",
                                        parameters),
                              new Query($"SELECT {selectList} FROM {table} WHERE {whereSql} ORDER BY {orderColumn} {orderDir} LIMIT @limit OFFSET @offset",
                                        orderParams),
                              page,
                              pageSize,
                              mapRow,
                              cancellationToken)
             .ConfigureAwait(false);
  }

  /// <summary>
  ///   Count the matches and read the requested page, using the given queries.
  /// </summary>
  /// <remarks>
  ///   The two queries are independent, so they run concurrently, each on its own pooled connection - a
  ///   single <see cref="NpgsqlConnection" /> can only run one command at a time. They are not in a shared
  ///   transaction and so see independent snapshots, exactly as they did when run sequentially on one
  ///   connection: under READ COMMITTED every statement takes its own snapshot either way.
  /// </remarks>
  /// <typeparam name="TRow">Type each row maps to</typeparam>
  /// <param name="connectionProvider">Provider of pooled connections</param>
  /// <param name="countQuery">Query returning the total number of matches as a single scalar</param>
  /// <param name="pageQuery">
  ///   Query returning one page of rows; its SQL must end with <c>LIMIT @limit OFFSET @offset</c>, whose
  ///   values are supplied from <paramref name="page" /> and <paramref name="pageSize" />
  /// </param>
  /// <param name="page">Zero-based index of the page to read</param>
  /// <param name="pageSize">Rows per page; when not positive no page is read and no rows are returned</param>
  /// <param name="mapRow">Maps the reader's current row to <typeparamref name="TRow" /></param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>The page's rows, and the total number of matches irrespective of paging</returns>
  public static async Task<(List<TRow> items, long totalCount)> ExecuteAsync<TRow>(NpgsqlConnectionProvider     connectionProvider,
                                                                                   Query                        countQuery,
                                                                                   Query                        pageQuery,
                                                                                   int                          page,
                                                                                   int                          pageSize,
                                                                                   Func<NpgsqlDataReader, TRow> mapRow,
                                                                                   CancellationToken            cancellationToken)
  {
    var countTask = RunAsync(connectionProvider,
                             countQuery,
                             async cmd => Convert.ToInt64(await cmd.ExecuteScalarAsync(cancellationToken)
                                                                   .ConfigureAwait(false)),
                             cancellationToken);

    if (pageSize <= 0)
    {
      return (new List<TRow>(), await countTask.ConfigureAwait(false));
    }

    var pageTask = RunAsync(connectionProvider,
                            pageQuery,
                            async cmd =>
                            {
                              cmd.Parameters.AddWithValue("limit",
                                                          pageSize);
                              cmd.Parameters.AddWithValue("offset",
                                                          (long)page * pageSize);

                              await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                                                .ConfigureAwait(false);

                              var items = new List<TRow>();
                              while (await reader.ReadAsync(cancellationToken)
                                                 .ConfigureAwait(false))
                              {
                                items.Add(mapRow.Invoke(reader));
                              }

                              return items;
                            },
                            cancellationToken);

    // WhenAll rather than awaiting in sequence, so a failure of either still observes both.
    await Task.WhenAll(countTask,
                       pageTask)
              .ConfigureAwait(false);

    return (await pageTask.ConfigureAwait(false), await countTask.ConfigureAwait(false));
  }

  /// <summary>
  ///   Run <paramref name="query" /> on its own connection and return whatever
  ///   <paramref name="execute" /> makes of the resulting command.
  /// </summary>
  /// <remarks>
  ///   The connection has to be held open for as long as the command runs, so the caller passes what to
  ///   do with the command rather than receiving it.
  /// </remarks>
  private static async Task<T> RunAsync<T>(NpgsqlConnectionProvider     connectionProvider,
                                           Query                        query,
                                           Func<NpgsqlCommand, Task<T>> execute,
                                           CancellationToken            cancellationToken)
  {
    await using var connection = await connectionProvider.GetConnectionAsync(cancellationToken)
                                                         .ConfigureAwait(false);
    await using var cmd = connection.CreateCommand();
    cmd.CommandText = query.Sql;
    SqlHelper.AddExpressionParameters(cmd,
                                      query.Parameters);

    return await execute.Invoke(cmd)
                        .ConfigureAwait(false);
  }
}
