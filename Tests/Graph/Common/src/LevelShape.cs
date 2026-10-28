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
using System.Globalization;
using System.Linq;

namespace ArmoniK.Samples.Graph.Common;

/// <summary>
///   How the tasks of a graph are spread over its levels (setting <c>TasksPerLevel</c>).
/// </summary>
/// <remarks>
///   <c>uniform</c> (as equal as possible), <c>increasing</c> (weights 1, 2, 3...), <c>decreasing</c> (weights ...,
///   3, 2, 1) or a comma separated list of weights such as <c>1,10,100</c>. When the list is shorter than the number
///   of levels, its last weight is repeated; when it is longer, the extra weights are ignored. The split is
///   deterministic: every level gets one task, the remaining tasks are shared in proportion to the weights (largest
///   remainder method, ties broken by the lowest level).
/// </remarks>
public sealed class LevelShape
{
  private readonly double[] weights_;

  private LevelShape(string   name,
                     double[] weights)
  {
    Name     = name;
    weights_ = weights;
  }

  /// <summary>Textual form of the shape</summary>
  public string Name { get; }

  /// <summary>
  ///   Parses a shape
  /// </summary>
  /// <param name="text">uniform, increasing, decreasing or a comma separated list of positive weights</param>
  /// <returns>
  ///   The parsed shape
  /// </returns>
  /// <exception cref="FormatException">The text is not a valid shape</exception>
  public static LevelShape Parse(string text)
  {
    ArgumentNullException.ThrowIfNull(text);
    var trimmed = text.Trim()
                      .ToLowerInvariant();
    switch (trimmed)
    {
      case "uniform":
      case "increasing":
      case "decreasing":
        return new LevelShape(trimmed,
                              Array.Empty<double>());
    }

    var weights = trimmed.Split(',')
                         .Select(w => double.TryParse(w.Trim(),
                                                      NumberStyles.Float,
                                                      CultureInfo.InvariantCulture,
                                                      out var value) && double.IsFinite(value) && value > 0
                                        ? value
                                        : throw new
                                            FormatException($"Invalid TasksPerLevel '{text}': expected uniform, increasing, decreasing or a comma separated list of strictly positive weights"))
                         .ToArray();

    return new LevelShape(string.Join(",",
                                      weights.Select(w => w.ToString("R",
                                                                     CultureInfo.InvariantCulture))),
                          weights);
  }

  /// <summary>
  ///   Splits <paramref name="tasks" /> tasks over <paramref name="levels" /> levels, each getting at least one task
  /// </summary>
  /// <param name="tasks">Number of tasks to spread</param>
  /// <param name="levels">Number of levels, between 1 and <paramref name="tasks" /></param>
  /// <returns>
  ///   The number of tasks of each level
  /// </returns>
  public int[] Split(int tasks,
                     int levels)
  {
    if (levels < 1 || levels > tasks)
    {
      throw new ArgumentOutOfRangeException(nameof(levels),
                                            levels,
                                            $"The number of levels should be between 1 and the number of tasks ({tasks})");
    }

    var weights = Enumerable.Range(0,
                                   levels)
                            .Select(Weight(levels))
                            .ToArray();

    var counts = Enumerable.Repeat(1,
                                   levels)
                           .ToArray();
    Distribute(counts,
               weights,
               tasks - levels);
    return counts;
  }

  /// <summary>
  ///   Adds <paramref name="amount" /> to <paramref name="counts" /> in proportion to <paramref name="weights" />
  ///   (largest remainder method, ties broken by the lowest index)
  /// </summary>
  /// <param name="counts">Counts to increase</param>
  /// <param name="weights">Positive weights, same length as <paramref name="counts" /></param>
  /// <param name="amount">Total to distribute</param>
  internal static void Distribute(int[]    counts,
                                  double[] weights,
                                  int      amount)
  {
    if (amount <= 0)
    {
      return;
    }

    var total      = weights.Sum();
    var remainders = new double[counts.Length];
    var given      = 0;
    for (var i = 0; i < counts.Length; i++)
    {
      var share = amount * weights[i] / total;
      var whole = (int)Math.Floor(share);
      counts[i]     += whole;
      given         += whole;
      remainders[i] =  share - whole;
    }

    var order = Enumerable.Range(0,
                                 counts.Length)
                          .OrderByDescending(i => remainders[i])
                          .ThenBy(i => i)
                          .ToArray();
    for (var j = 0; given < amount; j = (j + 1) % order.Length)
    {
      counts[order[j]]++;
      given++;
    }
  }

  private Func<int, double> Weight(int levels)
    => Name switch
       {
         "uniform"    => _ => 1.0,
         "increasing" => l => l      + 1.0,
         "decreasing" => l => levels - l,
         _ => l => weights_[Math.Min(l,
                                     weights_.Length - 1)],
       };

  /// <inheritdoc />
  public override string ToString()
    => Name;
}
