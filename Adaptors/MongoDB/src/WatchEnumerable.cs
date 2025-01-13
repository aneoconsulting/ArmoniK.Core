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
using System.Threading;

using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB;

/// <summary>
///   Class to convert a <see cref="IChangeStreamCursor{TInput}" /> into a <see cref="IAsyncEnumerator{TOutput}" />
/// </summary>
/// <typeparam name="TOutput">Output type</typeparam>
/// <typeparam name="TInput">Input type</typeparam>
public sealed class WatchEnumerable<TOutput, TInput> : IAsyncEnumerable<TOutput>
{
  private readonly Func<TInput, TOutput>       converter_;
  private readonly IChangeStreamCursor<TInput> cursor_;

  /// <summary>
  ///   Initializes a <see cref="IAsyncEnumerator{TOutput}" /> from a <see cref="IChangeStreamCursor{TInput}" />
  ///   with a conversion function to transform <typeparamref name="TInput" /> into <typeparamref name="TOutput" />
  /// </summary>
  /// <param name="cursor">Input change stream cursor</param>
  /// <param name="converter">Func to convert the input into the output</param>
  public WatchEnumerable(IChangeStreamCursor<TInput> cursor,
                         Func<TInput, TOutput>       converter)
  {
    cursor_    = cursor;
    converter_ = converter;
  }

  /// <inheritdoc />
  public IAsyncEnumerator<TOutput> GetAsyncEnumerator(CancellationToken cancellationToken = new())
    => new WatchEnumerator<TOutput, TInput>(cursor_,
                                            converter_,
                                            cancellationToken);
}
