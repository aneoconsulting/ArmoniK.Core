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
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Client.Options;
using ArmoniK.Api.Client.Submitter;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Utils;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Net.Client;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Tests.UploadBench;

/// <summary>
///   Client to perform the upload benchmark
/// </summary>
public class UploadBenchClient : IAsyncDisposable
{
  private readonly GrpcChannel             channel_;
  private readonly GrpcClient              clientOptions_;
  private readonly byte[]                  completeChunk_;
  private readonly CancellationTokenSource cts_;
  private readonly ILogger                 logger_;
  private readonly ILoggerFactory          loggerFactory_;
  private readonly ConcurrentBag<double[]> measurements_;
  private readonly Options.UploadBench     options_;
  private readonly byte[]                  partialChunk_;
  private          string                  session_;

  /// <summary>
  ///   Construct the benchmark client
  /// </summary>
  /// <param name="clientOptions">Options for the gRPC client</param>
  /// <param name="options">Options for the benchmark</param>
  /// <param name="loggerFactory">Logger factory</param>
  public UploadBenchClient(GrpcClient          clientOptions,
                           Options.UploadBench options,
                           ILoggerFactory      loggerFactory)
  {
    clientOptions_ = clientOptions;
    options_       = options;
    loggerFactory_ = loggerFactory;
    logger_        = loggerFactory.CreateLogger<UploadBenchClient>();
    channel_ = GrpcChannelFactory.CreateChannel(clientOptions,
                                                null,
                                                loggerFactory);

    completeChunk_ = new byte[options.MessageSize];
    partialChunk_ = options.ResultSize % options.MessageSize == 0
                      ? completeChunk_
                      : new byte[options.ResultSize % options.MessageSize];

    session_      = "";
    measurements_ = new ConcurrentBag<double[]>();
    cts_          = new CancellationTokenSource();
  }

  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    await cts_.CancelAsync()
              .ConfigureAwait(false);

    if (string.IsNullOrEmpty(session_))
    {
      return;
    }

    var client = new Sessions.SessionsClient(channel_);
    await client.CancelSessionAsync(new CancelSessionRequest
                                    {
                                      SessionId = session_,
                                    },
                                    cancellationToken: CancellationToken.None)
                .ConfigureAwait(false);
    logger_.LogInformation("Session cancelled {SessionId}",
                           session_);
    await client.PurgeSessionAsync(new PurgeSessionRequest
                                   {
                                     SessionId = session_,
                                   },
                                   cancellationToken: CancellationToken.None)
                .ConfigureAwait(false);

    logger_.LogInformation("Session purged {SessionId}",
                           session_);
    cts_.Dispose();
    channel_.Dispose();
  }

  /// <summary>
  ///   Create a new session in which the benchmark will run
  /// </summary>
  public async Task CreateSessionAsync()
  {
    var response = await new Sessions.SessionsClient(channel_).CreateSessionAsync(new CreateSessionRequest
                                                                                  {
                                                                                    DefaultTaskOption = new TaskOptions
                                                                                                        {
                                                                                                          PartitionId = "",
                                                                                                          MaxDuration = new Duration
                                                                                                                        {
                                                                                                                          Seconds = 1,
                                                                                                                        },
                                                                                                          MaxRetries = 1,
                                                                                                          Priority   = 1,
                                                                                                        },
                                                                                  },
                                                                                  cancellationToken: cts_.Token)
                                                              .ConfigureAwait(false);

    session_ = response.SessionId;

    logger_.LogInformation("Session created {SessionId}",
                           session_);
  }

  /// <summary>
  ///   Run a single benchmark runner
  /// </summary>
  /// <param name="threadId">Id of the thread that would run the benchmark</param>
  private async Task Run(int threadId)
  {
    // Open a new channel for this runner
    var channel = await GrpcChannelFactory.CreateChannelAsync(clientOptions_,
                                                              loggerFactory: loggerFactory_,
                                                              cancellationToken: cts_.Token)
                                          .ConfigureAwait(false);
    var client = new Results.ResultsClient(channel);

    // Results are shared among all the threads
    var n = options_.NbResults / options_.NbThreads;
    if (threadId < options_.NbResults % options_.NbThreads)
    {
      n += 1;
    }

    // Repeat the benchmark multiple times inside the same runner
    for (var r = 0; r < options_.Repeats; r += 1)
    {
      // Create metadata for all the results to be uploaded during the run
      var results = await client.CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest
                                                            {
                                                              SessionId = session_,
                                                              Results =
                                                              {
                                                                Enumerable.Range(0,
                                                                                 n)
                                                                          .Select(i => new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                                       {
                                                                                         Name           = $"result-{threadId}-{r}-{i}",
                                                                                         ManualDeletion = false,
                                                                                       }),
                                                              },
                                                            },
                                                            cancellationToken: cts_.Token)
                                .ConfigureAwait(false);

      logger_.LogDebug("Created {ResultsCount} results for {ThreadId}",
                       results.Results.Count,
                       threadId);

      try
      {
        var measurements = new double[n];
        var t0           = Stopwatch.GetTimestamp();

        // Upload sequentially all the results for this runner
        for (var i = 0; i < n; i += 1)
        {
          var stream = client.UploadResultData();
          await stream.RequestStream.WriteAsync(new UploadResultDataRequest
                                                {
                                                  Id = new UploadResultDataRequest.Types.ResultIdentifier
                                                       {
                                                         SessionId = results.Results[i].SessionId,
                                                         ResultId  = results.Results[i].ResultId,
                                                       },
                                                },
                                                cts_.Token)
                      .ConfigureAwait(false);

          var size = options_.ResultSize;
          while (size > options_.MessageSize)
          {
            await stream.RequestStream.WriteAsync(new UploadResultDataRequest
                                                  {
                                                    DataChunk = UnsafeByteOperations.UnsafeWrap(completeChunk_),
                                                  },
                                                  cts_.Token)
                        .ConfigureAwait(false);
            size -= options_.MessageSize;
          }

          await stream.RequestStream.WriteAsync(new UploadResultDataRequest
                                                {
                                                  DataChunk = UnsafeByteOperations.UnsafeWrap(partialChunk_),
                                                },
                                                cts_.Token)
                      .ConfigureAwait(false);

          await stream.RequestStream.CompleteAsync()
                      .ConfigureAwait(false);
          await stream.ResponseAsync.ConfigureAwait(false);
          var t1 = Stopwatch.GetTimestamp();
          measurements[i] = (double)(t1 - t0) / Stopwatch.Frequency;
          t0              = t1;
        }

        measurements_.Add(measurements);
      }
      finally
      {
        try
        {
          // Delete the data for all the results that were uploaded during this run
          await client.DeleteResultsDataAsync(new DeleteResultsDataRequest
                                              {
                                                SessionId = session_,
                                                ResultId =
                                                {
                                                  results.Results.Select(result => result.ResultId),
                                                },
                                              },
                                              cancellationToken: CancellationToken.None)
                      .ConfigureAwait(false);

          logger_.LogDebug("Deleted {ResultsCount} results for {ThreadId}",
                           results.Results.Count,
                           threadId);
        }
        catch (Exception e)
        {
          logger_.LogError(e,
                           "Failed to delete {ResultsCount} results for {ThreadId}",
                           results.Results.Count,
                           threadId);
        }
      }
    }
  }

  /// <summary>
  ///   Run the benchmark on multiple threads
  /// </summary>
  public Task RunAll()
    => Enumerable.Range(0,
                        options_.NbThreads)
                 .ParallelForEach(new ParallelTaskOptions
                                  {
                                    ParallelismLimit  = 0,
                                    CancellationToken = CancellationToken.None,
                                    Unordered         = true,
                                  },
                                  Run);

  /// <summary>
  ///   Print the measurements that were collected during the run
  /// </summary>
  public void PrintMeasurements()
  {
    var measurements = measurements_.SelectMany(m => m.Select(x => x))
                                    .ToArray();
    measurements.Sort();

    double s1 = 0;
    double s2 = 0;

    var minIndex           = 0;
    var firstDecileIndex   = measurements.Length     / 10;
    var firstQuartileIndex = measurements.Length     / 4;
    var medianIndex        = measurements.Length     / 2;
    var lastQuartileIndex  = 3 * measurements.Length / 4;
    var lastDecileIndex    = 9 * measurements.Length / 10;
    var maxIndex           = measurements.Length - 1;

    foreach (var measurement in measurements[firstDecileIndex..lastDecileIndex])
    {
      s1 += measurement;
      s2 += measurement * measurement;
    }

    double n        = lastDecileIndex - firstDecileIndex;
    var    mean     = s1 / n;
    var    variance = s2 / n - mean * mean;

    logger_.LogInformation("Upload time: {Mean} +- {Stderr} [0%: {Min}; 10%: {FirstDecile}; 25%: {FirstQuartile}; 50%: {Median}; 75%: {LastQuartile}; 90%: {LastDecile}; 100%: {Max}] {NbResults} results",
                           mean,
                           Math.Sqrt(variance),
                           measurements[minIndex],
                           measurements[firstDecileIndex],
                           measurements[firstQuartileIndex],
                           measurements[medianIndex],
                           measurements[lastQuartileIndex],
                           measurements[lastDecileIndex],
                           measurements[maxIndex],
                           measurements.Length);
  }

  /// <summary>
  ///   Cancel the benchmark runners
  /// </summary>
  public void Cancel()
    => cts_.Cancel();
}
