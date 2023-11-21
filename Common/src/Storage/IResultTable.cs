// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Exceptions;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Interface for managing results in database
/// </summary>
public interface IResultTable : IInitializable
{
  /// <summary>
  ///   Logger used to produce logs for this class
  /// </summary>
  public ILogger Logger { get; }

  /// <summary>
  ///   Change ownership (in batch) of the results in the given request
  /// </summary>
  /// <param name="sessionId">Session id of the session using the results</param>
  /// <param name="oldTaskId">Task Id of the previous owner</param>
  /// <param name="requests">Change ownership requests that will be executed</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task ChangeResultOwnership(string                                    sessionId,
                             string                                    oldTaskId,
                             IEnumerable<ChangeResultOwnershipRequest> requests,
                             CancellationToken                         cancellationToken);

  /// <summary>
  ///   Inserts the given results in the database
  /// </summary>
  /// <param name="results">Results that will be inserted into the database</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task Create(IEnumerable<Result> results,
              CancellationToken   cancellationToken = default);

  /// <summary>
  ///   Add the tasks Ids to the list of reverse dependencies of the given results
  /// </summary>
  /// <param name="sessionId">Id of the session containing the result</param>
  /// <param name="resultIds">List of result Id to update</param>
  /// <param name="taskIds">List of task Id to add to each result dependents</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task AddTaskDependency(string              sessionId,
                         ICollection<string> resultIds,
                         ICollection<string> taskIds,
                         CancellationToken   cancellationToken = default);

  /// <summary>
  ///   Add the tasks Ids to the list of reverse dependencies of the given results
  /// </summary>
  /// <param name="sessionId">Id of the session containing the result</param>
  /// <param name="dependencies">Enumeration of dependencies between results and tasks</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task AddTaskDependencies(string                                                                    sessionId,
                           IEnumerable<(ICollection<string> resultIds, ICollection<string> taskIds)> dependencies,
                           CancellationToken                                                         cancellationToken = default);

  /// <summary>
  ///   Delete the results from the database
  /// </summary>
  /// <param name="session">id of the session containing the result</param>
  /// <param name="key">id of the result to be deleted</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task DeleteResult(string            session,
                    string            key,
                    CancellationToken cancellationToken = default);

  /// <summary>
  ///   Delete all the results from a session
  /// </summary>
  /// <param name="sessionId">id of the session containing the result</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task DeleteResults(string            sessionId,
                     CancellationToken cancellationToken = default);

  /// <summary>
  ///   Get the results from a filter and convert it in the given type
  /// </summary>
  /// <param name="filter">Filter to select results</param>
  /// <param name="convertor">Expression to convert result into another type</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Collection of results metadata from the database
  /// </returns>
  IAsyncEnumerable<T> GetResults<T>(Expression<Func<Result, bool>> filter,
                                    Expression<Func<Result, T>>    convertor,
                                    CancellationToken              cancellationToken = default);

  /// <summary>
  ///   List all results matching the given request
  /// </summary>
  /// <param name="filter">Filter to select results</param>
  /// <param name="orderField">Select the field that will be used to order the results</param>
  /// <param name="ascOrder">Is the order ascending</param>
  /// <param name="page">The page of results to retrieve</param>
  /// <param name="pageSize">The number of results pages</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Collection of results metadata that matched the filter and total number of results without paging
  /// </returns>
  Task<(IEnumerable<Result> results, int totalCount)> ListResultsAsync(Expression<Func<Result, bool>>    filter,
                                                                       Expression<Func<Result, object?>> orderField,
                                                                       bool                              ascOrder,
                                                                       int                               page,
                                                                       int                               pageSize,
                                                                       CancellationToken                 cancellationToken = default);

  /// <summary>
  ///   Set Task that should produce the result
  /// </summary>
  /// <param name="sessionId"></param>
  /// <param name="requests">Results to update with the associated task id</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task SetTaskOwnership(string                                        sessionId,
                        ICollection<(string resultId, string taskId)> requests,
                        CancellationToken                             cancellationToken = default);

  /// <summary>
  ///   Get the result from its id
  /// </summary>
  /// <param name="sessionId">id of the session containing the result</param>
  /// <param name="key">id of the result to be retrieved</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Result metadata from the database
  /// </returns>
  public async Task<Result> GetResult(string            sessionId,
                                      string            key,
                                      CancellationToken cancellationToken = default)
  {
    try
    {
      return await GetResults(result => result.ResultId == key,
                              result => result,
                              cancellationToken)
                   .SingleAsync(cancellationToken)
                   .ConfigureAwait(false);
    }
    catch (InvalidOperationException)
    {
      throw new ResultNotFoundException($"Key '{key}' not found");
    }
  }

  // TODO Should be compatible with EFCORE : https://learn.microsoft.com/en-us/ef/core/saving/execute-insert-update-delete#updating-multiple-properties
  /// <summary>
  ///   Update one result with the given new values
  /// </summary>
  /// <param name="sessionId">Id of the session where the result to be updated is</param>
  /// <param name="resultId">Id of the result to be updated</param>
  /// <param name="updates">Collection of fields to update and their new value</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The result metadata before the update
  /// </returns>
  Task<Result> UpdateOneResult(string                                                                      sessionId,
                               string                                                                      resultId,
                               ICollection<(Expression<Func<Result, object?>> selector, object? newValue)> updates,
                               CancellationToken                                                           cancellationToken = default);


  /// <summary>
  ///   Update the results matching the filter with the given new values
  /// </summary>
  /// <param name="filter">Filter to select the results to update</param>
  /// <param name="updates">Collection of fields to update and their new value</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The number of results matched
  /// </returns>
  Task<long> UpdateManyResults(Expression<Func<Result, bool>>                                              filter,
                               ICollection<(Expression<Func<Result, object?>> selector, object? newValue)> updates,
                               CancellationToken                                                           cancellationToken = default);

  /// <summary>
  ///   Data structure to hold the results id and the new owner of the results in order to make batching easier
  /// </summary>
  /// <param name="Keys">Ids of the results that will change owner</param>
  /// <param name="NewTaskId">Task id of the new owner</param>
  public record ChangeResultOwnershipRequest(IEnumerable<string> Keys,
                                             string              NewTaskId);
}
