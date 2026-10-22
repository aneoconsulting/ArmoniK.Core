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
using System.Globalization;
using System.Linq;

namespace ArmoniK.Samples.Graph.Common;

/// <summary>
///   Kinds of distributions understood by <see cref="Distribution" />
/// </summary>
public enum DistributionKind
{
  /// <summary>Always the same value: <c>const:V</c></summary>
  Const,

  /// <summary>Uniform between two bounds: <c>uniform:MIN:MAX</c></summary>
  Uniform,

  /// <summary>Gaussian: <c>normal:MEAN:STDDEV</c></summary>
  Normal,

  /// <summary>Log-normal, MEDIAN * exp(SHAPE * N(0,1)): <c>lognormal:MEDIAN:SHAPE</c></summary>
  LogNormal,

  /// <summary>Exponential: <c>exponential:MEAN</c></summary>
  Exponential,

  /// <summary>Poisson: <c>poisson:MEAN</c></summary>
  Poisson,
}

/// <summary>
///   Random distribution used for every numeric setting of the graph.
/// </summary>
/// <remarks>
///   <para>
///     Syntax: <c>const:V</c>, <c>uniform:MIN:MAX</c>, <c>normal:MEAN:STDDEV</c>, <c>lognormal:MEDIAN:SHAPE</c>,
///     <c>exponential:MEAN</c> or <c>poisson:MEAN</c>, optionally followed by <c>:min=X</c> and/or <c>:max=Y</c>
///     to clamp the drawn values (e.g. <c>lognormal:100:1:max=5000</c>). A plain number <c>V</c> is a shortcut for
///     <c>const:V</c>.
///   </para>
///   <para>
///     Counts are rounded to the nearest integer and clamped to a lower bound by <see cref="SampleCount" />. For
///     counts, <c>uniform:MIN:MAX</c> with integer bounds draws each integer between MIN and MAX (inclusive) with the
///     same probability.
///   </para>
/// </remarks>
public sealed class Distribution
{
  private Distribution(DistributionKind kind,
                       double           first,
                       double           second,
                       double?          min,
                       double?          max)
  {
    Kind   = kind;
    First  = first;
    Second = second;
    Min    = min;
    Max    = max;
  }

  /// <summary>Kind of the distribution</summary>
  public DistributionKind Kind { get; }

  /// <summary>First parameter (value, minimum, mean or median depending on the kind)</summary>
  public double First { get; }

  /// <summary>Second parameter (maximum, standard deviation or shape depending on the kind), 0 if unused</summary>
  public double Second { get; }

  /// <summary>Lower bound applied to the drawn values, if any</summary>
  public double? Min { get; }

  /// <summary>Upper bound applied to the drawn values, if any</summary>
  public double? Max { get; }

  /// <summary>
  ///   Creates a constant distribution
  /// </summary>
  /// <param name="value">The constant</param>
  /// <returns>
  ///   The distribution that always gives <paramref name="value" />
  /// </returns>
  public static Distribution Constant(double value)
    => new(DistributionKind.Const,
           value,
           0,
           null,
           null);

  /// <summary>
  ///   Parses a distribution
  /// </summary>
  /// <param name="text">Distribution to parse, see the remarks of <see cref="Distribution" /></param>
  /// <returns>
  ///   The parsed distribution
  /// </returns>
  /// <exception cref="FormatException">The text is not a valid distribution</exception>
  public static Distribution Parse(string text)
  {
    ArgumentNullException.ThrowIfNull(text);

    var trimmed = text.Trim();
    if (TryParseNumber(trimmed,
                       out var constant))
    {
      return Constant(constant);
    }

    var     tokens     = trimmed.Split(':');
    var     parameters = new List<double>();
    double? min        = null;
    double? max        = null;

    foreach (var token in tokens.Skip(1))
    {
      var eq = token.IndexOf('=');
      if (eq < 0)
      {
        parameters.Add(ParseNumber(token,
                                   text));
        continue;
      }

      var name  = token[..eq].Trim();
      var value = ParseNumber(token[(eq + 1)..],
                              text);
      switch (name.ToLowerInvariant())
      {
        case "min":
          min = value;
          break;
        case "max":
          max = value;
          break;
        default:
          throw new FormatException($"Unknown bound '{name}' in distribution '{text}', expected min=X or max=Y");
      }
    }

    var kind = tokens[0]
               .Trim()
               .ToLowerInvariant() switch
               {
                 "const" or "constant" => DistributionKind.Const,
                 "uniform"             => DistributionKind.Uniform,
                 "normal"              => DistributionKind.Normal,
                 "lognormal"           => DistributionKind.LogNormal,
                 "exponential" or "exp" => DistributionKind.Exponential,
                 "poisson"             => DistributionKind.Poisson,
                 _ => throw new FormatException($"Unknown distribution '{tokens[0]}' in '{text}', expected const, uniform, normal, lognormal, exponential or poisson"),
               };

    var expected = kind switch
                   {
                     DistributionKind.Const       => 1,
                     DistributionKind.Exponential => 1,
                     DistributionKind.Poisson     => 1,
                     _                            => 2,
                   };

    if (parameters.Count != expected)
    {
      throw new FormatException($"Distribution '{text}' expects {expected} parameter(s), got {parameters.Count}");
    }

    var first  = parameters[0];
    var second = expected > 1
                   ? parameters[1]
                   : 0;

    var error = kind switch
                {
                  DistributionKind.Uniform when first > second => "MIN must be lower or equal to MAX",
                  DistributionKind.Normal when second < 0      => "STDDEV must be positive",
                  DistributionKind.LogNormal when first <= 0   => "MEDIAN must be strictly positive",
                  DistributionKind.LogNormal when second < 0   => "SHAPE must be positive",
                  DistributionKind.Exponential when first <= 0 => "MEAN must be strictly positive",
                  DistributionKind.Poisson when first < 0      => "MEAN must be positive",
                  _ when min > max                             => "min must be lower or equal to max",
                  _                                            => null,
                };

    if (error is not null)
    {
      throw new FormatException($"Invalid distribution '{text}': {error}");
    }

    return new Distribution(kind,
                            first,
                            second,
                            min,
                            max);
  }

  /// <summary>
  ///   Draws a value, clamped by the optional bounds
  /// </summary>
  /// <param name="random">Source of randomness</param>
  /// <returns>
  ///   The drawn value
  /// </returns>
  public double Sample(DeterministicRandom random)
  {
    var value = Kind switch
                {
                  DistributionKind.Const       => First,
                  DistributionKind.Uniform     => First + (Second - First) * random.NextDouble(),
                  DistributionKind.Normal      => First + Second * random.NextStandardNormal(),
                  DistributionKind.LogNormal   => First * Math.Exp(Second * random.NextStandardNormal()),
                  DistributionKind.Exponential => -First * Math.Log(random.NextDoubleNonZero()),
                  DistributionKind.Poisson     => SamplePoisson(random),
                  _                            => throw new InvalidOperationException($"Unknown distribution kind {Kind}"),
                };

    return Clamp(value);
  }

  /// <summary>
  ///   Draws a value clamped at 0, for durations and sizes
  /// </summary>
  /// <param name="random">Source of randomness</param>
  /// <returns>
  ///   The drawn value, positive
  /// </returns>
  public double SampleNonNegative(DeterministicRandom random)
    => Math.Max(0,
                Sample(random));

  /// <summary>
  ///   Draws a count: the value is rounded to the nearest integer and clamped to <paramref name="minimum" />
  /// </summary>
  /// <param name="random">Source of randomness</param>
  /// <param name="minimum">Smallest count returned (1 for task counts, 0 for dependency counts)</param>
  /// <returns>
  ///   The drawn count
  /// </returns>
  public int SampleCount(DeterministicRandom random,
                         int                 minimum)
  {
    double value;
    if (Kind == DistributionKind.Uniform && IsInteger(First) && IsInteger(Second) && Second - First < int.MaxValue)
    {
      value = Clamp(First + random.NextInt((int)(Second - First) + 1));
    }
    else
    {
      value = Math.Round(Sample(random),
                         MidpointRounding.AwayFromZero);
    }

    if (double.IsNaN(value) || value < minimum)
    {
      return minimum;
    }

    return value >= int.MaxValue
             ? int.MaxValue
             : (int)value;
  }

  /// <inheritdoc />
  public override string ToString()
  {
    var text = Kind switch
               {
                 DistributionKind.Const       => $"const:{Format(First)}",
                 DistributionKind.Uniform     => $"uniform:{Format(First)}:{Format(Second)}",
                 DistributionKind.Normal      => $"normal:{Format(First)}:{Format(Second)}",
                 DistributionKind.LogNormal   => $"lognormal:{Format(First)}:{Format(Second)}",
                 DistributionKind.Exponential => $"exponential:{Format(First)}",
                 DistributionKind.Poisson     => $"poisson:{Format(First)}",
                 _                            => Kind.ToString(),
               };

    if (Min is not null)
    {
      text += $":min={Format(Min.Value)}";
    }

    if (Max is not null)
    {
      text += $":max={Format(Max.Value)}";
    }

    return text;
  }

  private double Clamp(double value)
  {
    if (Min is not null && value < Min.Value)
    {
      value = Min.Value;
    }

    if (Max is not null && value > Max.Value)
    {
      value = Max.Value;
    }

    return value;
  }

  private double SamplePoisson(DeterministicRandom random)
  {
    if (First > 30)
    {
      // Normal approximation, accurate enough for large means
      return Math.Max(0,
                      Math.Round(First + Math.Sqrt(First) * random.NextStandardNormal(),
                                 MidpointRounding.AwayFromZero));
    }

    // Knuth's algorithm
    var limit   = Math.Exp(-First);
    var k       = 0;
    var product = random.NextDouble();
    while (product > limit)
    {
      k++;
      product *= random.NextDouble();
    }

    return k;
  }

  private static bool IsInteger(double value)
    => Math.Abs(value - Math.Round(value)) < 1e-9;

  private static string Format(double value)
    => value.ToString("R",
                      CultureInfo.InvariantCulture);

  private static bool TryParseNumber(string     text,
                                     out double value)
    => double.TryParse(text,
                       NumberStyles.Float,
                       CultureInfo.InvariantCulture,
                       out value) && double.IsFinite(value);

  private static double ParseNumber(string text,
                                    string distribution)
    => TryParseNumber(text.Trim(),
                      out var value)
         ? value
         : throw new FormatException($"Invalid number '{text}' in distribution '{distribution}'");
}
