using System;
using System.Threading;
using System.Threading.Tasks;

using DnsClient.Internal;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace ArmoniK.Compute.PollingAgent
{
  public class Worker : BackgroundService
  {
    private readonly Pollster        pollster_;

    public Worker(Pollster pollster) => pollster_ = pollster;
    
    protected override Task ExecuteAsync(CancellationToken stoppingToken) => pollster_.MainLoop(stoppingToken);
  }
}
