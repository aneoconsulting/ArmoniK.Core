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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace ArmoniK.Core.Adapters.PostgresSQL.Common;

internal static class WalHelpers
{
  /// <summary>
  ///   Iterates a WAL replication tuple, returning the values of <paramref name="needed" /> columns as strings.
  ///   Every column is consumed (mandatory to advance the replication stream), whether or not it is needed.
  /// </summary>
  internal static async Task<Dictionary<string, string?>> ReadTextColumns(ReplicationTuple  tuple,
                                                                           CancellationToken cancellationToken,
                                                                           params string[]   needed)
  {
    var neededSet = new HashSet<string>(needed,
                                       StringComparer.Ordinal);
    var result = new Dictionary<string, string?>(needed.Length,
                                                 StringComparer.Ordinal);
    await foreach (var val in tuple)
    {
      var name = val.GetFieldName();
      if (val.IsDBNull || val.IsUnchangedToastedValue)
      {
        if (neededSet.Contains(name))
        {
          result[name] = null;
        }
      }
      else if (neededSet.Contains(name))
      {
        result[name] = await val.Get<string>(cancellationToken)
                                .ConfigureAwait(false);
      }
      else
      {
        await val.Get(cancellationToken)
                 .ConfigureAwait(false);
      }
    }

    return result;
  }

  /// <summary>
  ///   Consumes the old-key or old-row tuple of an UPDATE message so the stream position
  ///   advances past it before the caller reads <see cref="UpdateMessage.NewRow" />.
  ///   With DEFAULT replica identity, PostgreSQL sends the primary-key columns as an old-key
  ///   tuple ('K' tag), which Npgsql surfaces as <see cref="IndexUpdateMessage" />.
  ///   With REPLICA IDENTITY FULL it is a <see cref="FullUpdateMessage" />.
  ///   The base <see cref="UpdateMessage" /> has no old tuple and needs no pre-consumption.
  /// </summary>
  internal static async Task ConsumeOldTuple(UpdateMessage     message,
                                             CancellationToken cancellationToken)
  {
    if (message is FullUpdateMessage full)
    {
      await ReadTextColumns(full.OldRow,
                            cancellationToken)
        .ConfigureAwait(false);
    }
    else if (message is IndexUpdateMessage idx)
    {
      await ReadTextColumns(idx.Key,
                            cancellationToken)
        .ConfigureAwait(false);
    }
  }

  /// <summary>
  ///   Consumes and discards all tuple data in a WAL message.
  ///   Must be called for every message whose tuple is not otherwise read,
  ///   to keep the replication stream position advancing correctly.
  /// </summary>
  internal static async Task ConsumeMessage(PgOutputReplicationMessage message,
                                            CancellationToken          cancellationToken)
  {
    switch (message)
    {
      case InsertMessage insert:
        await ReadTextColumns(insert.NewRow,
                              cancellationToken)
          .ConfigureAwait(false);
        break;

      case FullUpdateMessage update:
        await ReadTextColumns(update.OldRow,
                              cancellationToken)
          .ConfigureAwait(false);
        await ReadTextColumns(update.NewRow,
                              cancellationToken)
          .ConfigureAwait(false);
        break;

      case IndexUpdateMessage update:
        await ReadTextColumns(update.Key,
                              cancellationToken)
          .ConfigureAwait(false);
        await ReadTextColumns(update.NewRow,
                              cancellationToken)
          .ConfigureAwait(false);
        break;

      case UpdateMessage update:
        await ReadTextColumns(update.NewRow,
                              cancellationToken)
          .ConfigureAwait(false);
        break;

      case FullDeleteMessage delete:
        await ReadTextColumns(delete.OldRow,
                              cancellationToken)
          .ConfigureAwait(false);
        break;

      case KeyDeleteMessage delete:
        await ReadTextColumns(delete.Key,
                              cancellationToken)
          .ConfigureAwait(false);
        break;
    }
  }
}
