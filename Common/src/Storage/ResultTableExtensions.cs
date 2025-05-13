// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

using ArmoniK.Core.Common.Exceptions;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Storage;

public static class ResultTableExtensions
{
  /// <summary>
  ///   Abort the results of the given task
  /// </summary>
  /// <param name="resultTable">Interface to manage results</param>
  /// <param name="sessionId">id of the session containing the results</param>
  /// <param name="ownerTaskId">id of the task from which abort the results</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task AbortTaskResults(this IResultTable resultTable,
                                            string            sessionId,
                                            string            ownerTaskId,
                                            CancellationToken cancellationToken = default)
  {
    await resultTable.UpdateManyResults(result => result.OwnerTaskId == ownerTaskId,
                                        new UpdateDefinition<Result>().Set(data => data.Status,
                                                                           ResultStatus.Aborted)
                                                                      .Set(data => data.CompletionDate,
                                                                           DateTime.UtcNow),
                                        cancellationToken)
                     .ConfigureAwait(false);

    resultTable.Logger.LogDebug("Abort results from {owner}",
                                ownerTaskId);
  }

  /// <summary>
  ///   Abort the results of the given session
  /// </summary>
  /// <param name="resultTable">Interface to manage results</param>
  /// <param name="sessionId">id of the session containing the results</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task AbortSessionResults(this IResultTable resultTable,
                                               string            sessionId,
                                               CancellationToken cancellationToken = default)
  {
    await resultTable.UpdateManyResults(result => result.SessionId == sessionId && result.Status == ResultStatus.Created,
                                        new UpdateDefinition<Result>().Set(data => data.Status,
                                                                           ResultStatus.Aborted),
                                        cancellationToken)
                     .ConfigureAwait(false);

    resultTable.Logger.LogDebug("Abort results from {session}",
                                sessionId);
  }


  /// <summary>
  ///   Updates in bulk results
  /// </summary>
  /// <param name="resultTable">Interface to manage result lifecycle</param>
  /// <param name="bulkUpdates">Enumeration of updates with the resultId they apply on</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The number of result matched
  /// </returns>
  public static Task<long> BulkUpdateResults(this IResultTable                                                resultTable,
                                             IEnumerable<(string resultId, UpdateDefinition<Result> updates)> bulkUpdates,
                                             CancellationToken                                                cancellationToken)
    => resultTable.BulkUpdateResults(bulkUpdates.Select(item => ((Expression<Func<Result, bool>>)(task => task.ResultId == item.resultId), item.updates)),
                                     cancellationToken);

  /// <summary>
  ///   Complete result
  /// </summary>
  /// <param name="resultTable">Interface to manage results</param>
  /// <param name="sessionId">id of the session containing the results</param>
  /// <param name="resultId">Id of the result to complete</param>
  /// <param name="size">Size of the result to complete</param>
  /// <param name="opaqueId">Opaque unique identifier representing the object</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The new version of the result metadata
  /// </returns>
  /// <exception cref="ResultNotFoundException">when result to update is not found</exception>
  public static async Task<Result> CompleteResult(this IResultTable resultTable,
                                                  string            sessionId,
                                                  string            resultId,
                                                  long              size,
                                                  byte[]            opaqueId,
                                                  CancellationToken cancellationToken = default)
  {
    var result = await resultTable.UpdateOneResult(resultId,
                                                   new UpdateDefinition<Result>().Set(data => data.Status,
                                                                                      ResultStatus.Completed)
                                                                                 .Set(data => data.Size,
                                                                                      size)
                                                                                 .Set(data => data.OpaqueId,
                                                                                      opaqueId)
                                                                                 .Set(data => data.CompletionDate,
                                                                                      DateTime.UtcNow),
                                                   cancellationToken)
                                  .ConfigureAwait(false);

    resultTable.Logger.LogDebug("Update {result} to {status}",
                                resultId,
                                ResultStatus.Completed);

    return result with
           {
             Status = ResultStatus.Completed,
             Size = size,
             OpaqueId = opaqueId,
           };
  }

  /// <summary>
  ///   Complete many results in the database
  /// </summary>
  /// <param name="resultTable">Interface to manage results</param>
  /// <param name="sessionId">id of the session containing the results</param>
  /// <param name="resultId">Id of the result to complete</param>
  /// <param name="size">Size of the result to complete</param>
  /// <param name="opaqueId">Opaque unique identifier representing the object</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The new version of the result metadata
  /// </returns>
  /// <exception cref="ResultNotFoundException">when result to update is not found</exception>
  public static async Task CompleteManyResults(this IResultTable                                          resultTable,
                                               IEnumerable<(string resultId, long size, byte[] opaqueId)> results,
                                               CancellationToken                                          cancellationToken = default)
  {
    var now = DateTime.UtcNow;
    await resultTable.BulkUpdateResults(results.Select(r => (r.resultId, new UpdateDefinition<Result>().Set(result => result.Status,
                                                                                                            ResultStatus.Completed)
                                                                                                       .Set(result => result.OpaqueId,
                                                                                                            r.opaqueId)
                                                                                                       .Set(result => result.Size,
                                                                                                            r.size)
                                                                                                       .Set(result => result.CompletionDate,
                                                                                                            now))),
                                        cancellationToken)
                     .ConfigureAwait(false);
  }

  /// <summary>
  ///   Update result
  /// </summary>
  /// <param name="resultTable">Interface to manage results</param>
  /// <param name="sessionId">id of the session containing the results</param>
  /// <param name="ownerTaskId">id of the task owning the result</param>
  /// <param name="resultId">id of the result to be modified</param>
  /// <param name="size">Size of the result to be modified</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task SetResult(this IResultTable resultTable,
                                     string            sessionId,
                                     string            ownerTaskId,
                                     string            resultId,
                                     long              size,
                                     byte[]            opaqueId,
                                     CancellationToken cancellationToken = default)
  {
    var count = await resultTable.UpdateManyResults(result => result.ResultId == resultId && result.OwnerTaskId == ownerTaskId,
                                                    new UpdateDefinition<Result>().Set(result => result.Status,
                                                                                       ResultStatus.Completed)
                                                                                  .Set(result => result.Size,
                                                                                       size)
                                                                                  .Set(result => result.OpaqueId,
                                                                                       opaqueId)
                                                                                  .Set(result => result.CompletionDate,
                                                                                       DateTime.UtcNow),
                                                    cancellationToken)
                                 .ConfigureAwait(false);

    resultTable.Logger.LogDebug("Update {result} from {owner} to {status}",
                                resultId,
                                ownerTaskId,
                                ResultStatus.Completed);

    if (count == 0)
    {
      throw new ResultNotFoundException($"Result '{resultId}' was not found for '{ownerTaskId}'");
    }
  }

  /// <summary>
  ///   Get the results from a collection of ids
  /// </summary>
  /// <param name="resultTable">Interface to manage results</param>
  /// <param name="sessionId">id of the session containing the result</param>
  /// <param name="keys">Collection of id of the result to be retrieved</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Collection of results metadata from the database
  /// </returns>
  public static IAsyncEnumerable<Result> GetResults(this IResultTable   resultTable,
                                                    string              sessionId,
                                                    IEnumerable<string> keys,
                                                    CancellationToken   cancellationToken = default)
    => resultTable.GetResults(result => keys.Contains(result.ResultId),
                              result => result,
                              cancellationToken);

  /// <summary>
  ///   Get the list of task that depends on the result
  /// </summary>
  /// <param name="resultTable">Interface to manage results</param>
  /// <param name="sessionId">Id of the session containing the result</param>
  /// <param name="resultId">Id of the result</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Ids of the task dependent on this result
  /// </returns>
  public static async Task<IEnumerable<string>> GetDependents(this IResultTable resultTable,
                                                              string            sessionId,
                                                              string            resultId,
                                                              CancellationToken cancellationToken = default)
  {
    try
    {
      return await resultTable.GetResults(result => result.ResultId == resultId,
                                          result => result.DependentTasks,
                                          cancellationToken)
                              .Select(l => l.AsEnumerable())
                              .SingleAsync(cancellationToken)
                              .ConfigureAwait(false);
    }
    catch (InvalidOperationException e)
    {
      throw new ResultNotFoundException($"Result {resultId} not found",
                                        e);
    }
  }

  /// <summary>
  ///   Get the status of the given results
  /// </summary>
  /// <param name="resultTable">Interface to manage results</param>
  /// <param name="ids">ids of the results</param>
  /// <param name="sessionId">id of the session containing the results</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   A map between the ids of the results found and their status
  /// </returns>
  public static async Task<IEnumerable<ResultIdStatus>> GetResultStatus(this IResultTable   resultTable,
                                                                        IEnumerable<string> ids,
                                                                        string              sessionId,
                                                                        CancellationToken   cancellationToken = default)
    => await resultTable.GetResults(result => ids.Contains(result.ResultId),
                                    result => new ResultIdStatus(result.ResultId,
                                                                 result.Status),
                                    cancellationToken)
                        .ToListAsync(cancellationToken)
                        .ConfigureAwait(false);

  /// <summary>
  ///   Set the result status to <see cref="ResultStatus.DeletedData" />
  /// </summary>
  /// <param name="resultTable">Interface to manage results</param>
  /// <param name="resultId">Id of the result to update</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task MarkAsDeleted(this IResultTable resultTable,
                                         string            resultId,
                                         CancellationToken cancellationToken = default)
    => await resultTable.UpdateOneResult(resultId,
                                         new UpdateDefinition<Result>().Set(result => result.Status,
                                                                            ResultStatus.DeletedData)
                                                                       .Set(result => result.OpaqueId,
                                                                            Array.Empty<byte>()),
                                         cancellationToken)
                        .ConfigureAwait(false);
}
