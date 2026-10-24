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

namespace ArmoniK.Samples.Graph.Common;

/// <summary>
///   Kinds of <see cref="ParentChoice" />
/// </summary>
public enum ParentChoiceKind
{
  /// <summary>Every output of the chosen level has the same probability</summary>
  Uniform,

  /// <summary>
  ///   Rank based Zipf law: the output of index r of the chosen level is drawn with a probability proportional to
  ///   1 / (r + 1)^S, so a few outputs are depended on by many tasks
  /// </summary>
  Zipf,

  /// <summary>
  ///   Neighbours by relative position: a task at the relative position p of its level depends on the outputs closest to
  ///   the relative position p of the chosen level
  /// </summary>
  Local,
}

/// <summary>
///   How a task chooses the outputs it depends on inside a level (setting <c>ParentChoice</c>):
///   <c>uniform</c>, <c>zipf:S</c> or <c>local</c>.
/// </summary>
public sealed class ParentChoice
{
  private ParentChoice(ParentChoiceKind kind,
                       double           exponent)
  {
    Kind     = kind;
    Exponent = exponent;
  }

  /// <summary>Kind of choice</summary>
  public ParentChoiceKind Kind { get; }

  /// <summary>Exponent S of the Zipf law, 0 for the other kinds</summary>
  public double Exponent { get; }

  /// <summary>
  ///   Parses a parent choice
  /// </summary>
  /// <param name="text"><c>uniform</c>, <c>zipf:S</c> (S positive) or <c>local</c></param>
  /// <returns>
  ///   The parsed choice
  /// </returns>
  /// <exception cref="FormatException">The text is not valid</exception>
  public static ParentChoice Parse(string text)
  {
    ArgumentNullException.ThrowIfNull(text);
    var tokens = text.Trim()
                     .ToLowerInvariant()
                     .Split(':');
    switch (tokens[0])
    {
      case "uniform" when tokens.Length == 1:
        return new ParentChoice(ParentChoiceKind.Uniform,
                                0);
      case "local" when tokens.Length == 1:
        return new ParentChoice(ParentChoiceKind.Local,
                                0);
      case "zipf" when tokens.Length == 2 && double.TryParse(tokens[1],
                                                             NumberStyles.Float,
                                                             CultureInfo.InvariantCulture,
                                                             out var s) && double.IsFinite(s) && s >= 0:
        return new ParentChoice(ParentChoiceKind.Zipf,
                                s);
      default:
        throw new FormatException($"Invalid ParentChoice '{text}': expected uniform, zipf:S (S >= 0) or local");
    }
  }

  /// <inheritdoc />
  public override string ToString()
    => Kind switch
       {
         ParentChoiceKind.Zipf => $"zipf:{Exponent.ToString("R", CultureInfo.InvariantCulture)}",
         _                     => Kind.ToString()
                                      .ToLowerInvariant(),
       };
}
