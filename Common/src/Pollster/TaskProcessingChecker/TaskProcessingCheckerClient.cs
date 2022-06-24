using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster.TaskProcessingChecker;

public class TaskProcessingCheckerClient : ITaskProcessingChecker
{
  private readonly IHttpClientFactory                   httpClientFactory_;
  private readonly ILogger<TaskProcessingCheckerClient> logger_;

  public TaskProcessingCheckerClient(IHttpClientFactory                   httpClientFactory,
                                     ILogger<TaskProcessingCheckerClient> logger)
  {
    httpClientFactory_ = httpClientFactory;
    logger_            = logger;
  }

  public async Task<bool> Check(string            taskId,
                                string            ownerPodId,
                                CancellationToken cancellationToken)
  {
    logger_.LogTrace("Check if task is processing");
    var client = httpClientFactory_.CreateClient();

    try
    {
      var result = await client.GetStringAsync(ownerPodId,
                                               cancellationToken)
                               .ConfigureAwait(false);
      return result.Equals(taskId);
    }
    catch (InvalidOperationException ex)
    {
      logger_.LogWarning(ex,
                         "Cannot communicate with other pod");
      return false;
    }
    catch (Exception ex)
    {
      logger_.LogError(ex,
                       "Error while checking");
      throw;
    }
  }
}