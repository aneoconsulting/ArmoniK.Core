// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Control.Metrics;

public static partial class Program
{
  public class MetricGenerator
  {
    private readonly ITaskTable      taskTable_;
    private readonly Options.Metrics options_;

    public MetricGenerator(ITaskTable taskTable, Options.Metrics options)
    {
      taskTable_    = taskTable;
      options_      = options;
    }

    public async Task<string> GetMetrics()
    {
      var tasks = await taskTable_.CountAllTasksAsync().ToListAsync();

      var metricList = new MetricList
      {
        ApiVersion = options_.ApiVersion,
        Kind       = "MetricValueList",
        Metadata = new Dictionary<string, string>
        {
          {"selfLink", "/apis/custom.metrics.k8s.io/v1beta1"},
        },
        Items = new List<Metric>(),
      };

      var metricQueued = new Metric
      {
        DescribedObject = new ObjectDescription
        {
          ApiVersion          = options_.DescribedObject.ApiVersion,
          Kind                = "Service",
          Name                = options_.DescribedObject.Name,
          KubernetesNamespace = options_.DescribedObject.Namespace,
        },
        Timestamp  = DateTime.Now.ToString("s") + "Z",
        MetricName = "armonik_tasks_queued",
        Value      = 0,
      };

      foreach (var status in Enum.GetNames(typeof(TaskStatus)))
      {
        var metric = new Metric
        {
          DescribedObject = new ObjectDescription
          {
            ApiVersion          = options_.DescribedObject.ApiVersion,
            Kind                = "Service",
            Name                = options_.DescribedObject.Name,
            KubernetesNamespace = options_.DescribedObject.Namespace,
          },
          Timestamp = DateTime.Now.ToString("s") + "Z",
          MetricName = "armonik_tasks_" + status.ToLower(),
          Value = 0,
        };
        if (tasks.Any())
        {
          var statusCount = tasks.DistinctBy(c => c.Status.ToString() == status).FirstOrDefault(defaultValue: new TaskStatusCount
                                                                                                 (TaskStatus.Canceled, 0));
          if(statusCount.Count > 0)
          {
            metric.Value = statusCount.Count;
            if (statusCount.Status is TaskStatus.Creating or TaskStatus.Dispatched or TaskStatus.Processing or TaskStatus.Submitted)
            {
              metricQueued.Value += statusCount.Count;
            }
          }
        }

        metricList.Items.Add(metric);
      }
      metricList.Items.Add(metricQueued);
      return JsonSerializer.Serialize(metricList);
    }
  }

  public class MetricList
  {
    [JsonPropertyName("kind")]
    public string Kind { get; set; }

    [JsonPropertyName("apiVersion")]
    public string ApiVersion { get; set; }

    [JsonPropertyName("metadata")]
    public Dictionary<string, string> Metadata { get; set; }

    [JsonPropertyName("items")]
    public IList<Metric> Items { get; set; }
  }

  public class ObjectDescription
  {
    [JsonPropertyName("kind")]
    public string Kind { get; set; }

    [JsonPropertyName("namespace")]
    public string KubernetesNamespace { get; set; }

    [JsonPropertyName("name")]
    public string Name { get; set; }

    [JsonPropertyName("apiVersion")]
    public string ApiVersion { get; set; }
  }

  public class Metric
  {
    [JsonPropertyName("describedObject")]
    public ObjectDescription DescribedObject { get; set; }

    [JsonPropertyName("metricName")]
    public string MetricName { get; set; }

    [JsonPropertyName("timestamp")]
    public string Timestamp { get; set; }

    [JsonPropertyName("value")]
    public int Value { get; set; }
  }

}