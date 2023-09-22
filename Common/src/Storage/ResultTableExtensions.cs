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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.Exceptions;

namespace ArmoniK.Core.Common.Storage;

public static class ResultTableExtensions
{
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
  public static async Task<IEnumerable<GetResultStatusReply.Types.IdStatus>> GetResultStatus(this IResultTable   resultTable,
                                                                                             IEnumerable<string> ids,
                                                                                             string              sessionId,
                                                                                             CancellationToken   cancellationToken = default)
    => await resultTable.GetResults(result => ids.Contains(result.ResultId),
                                    result => new GetResultStatusReply.Types.IdStatus
                                              {
                                                ResultId = result.ResultId,
                                                Status   = result.Status,
                                              },
                                    cancellationToken)
                        .ToListAsync(cancellationToken)
                        .ConfigureAwait(false);
}
