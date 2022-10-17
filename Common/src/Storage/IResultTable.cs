// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Submitter;
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
  ///   Check the status of the given results
  /// </summary>
  /// <param name="sessionId">Session id of the session using the results</param>
  /// <param name="keys">Ids of the results that will be checked</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The number of results in each kind of status
  /// </returns>
  Task<IEnumerable<ResultStatusCount>> AreResultsAvailableAsync(string              sessionId,
                                                                IEnumerable<string> keys,
                                                                CancellationToken   cancellationToken = default);

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
  ///   Get the results from a collection of ids
  /// </summary>
  /// <param name="sessionId">id of the session containing the result</param>
  /// <param name="keys">Collection of id of the result to be retrieved</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Collection of results metadata from the database
  /// </returns>
  IAsyncEnumerable<Result> GetResults(string              sessionId,
                                      IEnumerable<string> keys,
                                      CancellationToken   cancellationToken = default);

  /// <summary>
  ///   List results from a session
  /// </summary>
  /// <param name="sessionId">id of the session containing the results</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   The ids of the results in the session
  /// </returns>
  IAsyncEnumerable<string> ListResultsAsync(string            sessionId,
                                            CancellationToken cancellationToken = default);

  /// <summary>
  ///   List all results matching the given request
  /// </summary>
  /// <param name="request">Result request describing the results to be listed </param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Collection of results metadata that matched the filter
  /// </returns>
  Task<IEnumerable<Result>> ListResultsAsync(ListResultsRequest request,
                                             CancellationToken  cancellationToken = default);

  /// <summary>
  ///   Update result with small payload
  /// </summary>
  /// <param name="sessionId">id of the session containing the results</param>
  /// <param name="ownerTaskId">id of the task owning the result</param>
  /// <param name="key">id of the result to be modified</param>
  /// <param name="smallPayload">payload data</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task SetResult(string            sessionId,
                 string            ownerTaskId,
                 string            key,
                 byte[]            smallPayload,
                 CancellationToken cancellationToken = default);

  /// <summary>
  ///   Update result
  /// </summary>
  /// <param name="sessionId">id of the session containing the results</param>
  /// <param name="ownerTaskId">id of the task owning the result</param>
  /// <param name="key">id of the result to be modified</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task SetResult(string            sessionId,
                 string            ownerTaskId,
                 string            key,
                 CancellationToken cancellationToken = default);

  /// <summary>
  ///   Get the status of the given results
  /// </summary>
  /// <param name="ids">ids of the results</param>
  /// <param name="sessionId">id of the session containing the results</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   A map between the ids of the results found and their status
  /// </returns>
  Task<IEnumerable<GetResultStatusReply.Types.IdStatus>> GetResultStatus(IEnumerable<string> ids,
                                                                         string              sessionId,
                                                                         CancellationToken   cancellationToken = default);

  /// <summary>
  ///   Abort the results of the given task
  /// </summary>
  /// <param name="sessionId">id of the session containing the results</param>
  /// <param name="ownerTaskId">id of the task from which abort the results</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task AbortTaskResults(string            sessionId,
                        string            ownerTaskId,
                        CancellationToken cancellationToken = default);

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
      return await GetResults(sessionId,
                              new[]
                              {
                                key,
                              },
                              cancellationToken)
                   .SingleAsync(cancellationToken)
                   .ConfigureAwait(false);
    }
    catch (InvalidOperationException)
    {
      throw new ResultNotFoundException($"Key '{key}' not found");
    }
  }

  /// <summary>
  ///   Data structure to hold the results id and the new owner of the results in order to make batching easier
  /// </summary>
  /// <param name="Keys">Ids of the results that will change owner</param>
  /// <param name="NewTaskId">Task id of the new owner</param>
  public record ChangeResultOwnershipRequest(IEnumerable<string> Keys,
                                             string              NewTaskId);
}
