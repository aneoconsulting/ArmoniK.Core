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

using ArmoniK.Utils.DocAttribute;

namespace ArmoniK.Core.Adapters.Broker;

/// <summary>
///   Options of the ArmoniK Broker queue adapter.
/// </summary>
[ExtractDocumentation("Options for the ArmoniK Broker")]
public class Broker
{
  /// <summary>
  ///   The name of the configuration section.
  /// </summary>
  public const string SettingSection = nameof(Broker);

  /// <summary>
  ///   Base URL of the broker, for example <c>http://broker:8080</c> (h2c) or <c>https://broker:8443</c>.
  /// </summary>
  public string Endpoint { get; set; } = string.Empty;

  /// <summary>
  ///   Use HTTP/2 (prior knowledge on <c>http://</c>). When false, HTTP/1.1 is used.
  /// </summary>
  public bool Http2 { get; set; } = true;

  /// <summary>
  ///   PEM file of the certificate authority that signed the server certificate. Empty to use the system store.
  /// </summary>
  public string CaFile { get; set; } = string.Empty;

  /// <summary>
  ///   Accept a server certificate whose name does not match the endpoint host (only with <see cref="CaFile" />).
  /// </summary>
  public bool AllowHostMismatch { get; set; }

  /// <summary>
  ///   PEM file of the client certificate for mutual TLS. Empty to disable client authentication.
  /// </summary>
  public string ClientCertificateFile { get; set; } = string.Empty;

  /// <summary>
  ///   PEM file of the private key of <see cref="ClientCertificateFile" />.
  /// </summary>
  public string ClientKeyFile { get; set; } = string.Empty;

  /// <summary>
  ///   Long poll duration of a pull. The Pollster calls pull in a loop, so this only bounds idle requests.
  /// </summary>
  public TimeSpan PullWait { get; set; } = TimeSpan.FromSeconds(10);

  /// <summary>
  ///   Period of the lease renewal of a consumer. Must stay well below the server lease (30 s by default).
  /// </summary>
  public TimeSpan RenewPeriod { get; set; } = TimeSpan.FromSeconds(10);

  /// <summary>
  ///   How long retryable failures (backpressure, overload, restart, network) are retried before an
  ///   operation fails. Failures are absorbed here so that they do not consume task retries in Core.
  /// </summary>
  public TimeSpan MaxRetryDuration { get; set; } = TimeSpan.FromMinutes(5);

  /// <summary>
  ///   Timeout of an enqueue, registration, acknowledgement or negative acknowledgement request.
  /// </summary>
  public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

  /// <summary>
  ///   Timeout of a lease renewal request; it must stay well below the server lease.
  /// </summary>
  public TimeSpan RenewTimeout { get; set; } = TimeSpan.FromSeconds(10);

  /// <summary>
  ///   Timeout of the requests reading the server limits and its health.
  /// </summary>
  public TimeSpan ProbeTimeout { get; set; } = TimeSpan.FromSeconds(5);

  /// <summary>
  ///   Time given to a pull beyond <see cref="PullWait" /> before it is abandoned.
  /// </summary>
  public TimeSpan PullTimeoutMargin { get; set; } = TimeSpan.FromSeconds(10);

  /// <summary>
  ///   First delay of the exponential back-off on retryable failures (protocol §5).
  /// </summary>
  public TimeSpan BackoffMin { get; set; } = TimeSpan.FromMilliseconds(100);

  /// <summary>
  ///   Largest delay of the exponential back-off on retryable failures (protocol §5).
  /// </summary>
  public TimeSpan BackoffMax { get; set; } = TimeSpan.FromSeconds(10);

  /// <summary>
  ///   Period of the HTTP/2 PING frames keeping idle connections alive through firewalls and NAT.
  /// </summary>
  public TimeSpan KeepAlivePingPeriod { get; set; } = TimeSpan.FromSeconds(30);

  /// <summary>
  ///   Timeout of a TCP connection establishment.
  /// </summary>
  public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(10);

  /// <summary>
  ///   Largest enqueue batch sent in one request. The limit the server returns (<c>GET /v1/limits</c>,
  ///   about 150 with its default 64 KiB body size) also applies.
  /// </summary>
  public int MaxBatchItems { get; set; } = 128;

  /// <summary>
  ///   Identifier of the compute node, used for task and data affinity. Defaults to the <c>NODE_NAME</c>
  ///   environment variable, then to the machine name. Set to <c>-</c> to disable affinity.
  /// </summary>
  public string NodeId { get; set; } = string.Empty;

  /// <summary>
  ///   Capacity of the local data cache of the node, in bytes; 0 disables affinity for this consumer.
  /// </summary>
  public long CacheCapacityBytes { get; set; } = 10L * 1024 * 1024 * 1024;

  /// <summary>
  ///   Task and data affinity: declare the node at registration, read the sizes of the data dependencies
  ///   at enqueue (one more database query per batch) and declare the outputs at acknowledgement.
  ///   Only useful when the agents keep data in their cache (<c>Pollster:CacheEvictionThreshold</c> above 0);
  ///   disabled by default, the broker then serves each priority in FIFO order.
  /// </summary>
  public bool Affinity { get; set; }
}
