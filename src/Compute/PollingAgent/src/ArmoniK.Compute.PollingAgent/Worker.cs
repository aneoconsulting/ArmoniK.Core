using System;
using System.Threading;
using System.Threading.Tasks;

using DnsClient.Internal;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Compute.PollingAgent
{
  public class Worker : BackgroundService
  {
    private readonly Pollster        pollster_;
    private readonly ILogger<Worker> logger_;

    public Worker(Pollster pollster, ILogger<Worker> logger)
    {
      pollster_ = pollster;
      logger_        = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => pollster_.MainLoop(stoppingToken);
  }
}
