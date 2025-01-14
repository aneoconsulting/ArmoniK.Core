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
using System.Linq.Expressions;

using Destructurama.Attributed;

using FluentValidation.Internal;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Express the updates of an object of type <typeparamref name="T" />.
/// </summary>
/// <typeparam name="T">Type on which the updates would be applied</typeparam>
public class UpdateDefinition<T>
{
  /// <summary>
  ///   List of setters
  /// </summary>
  public List<PropertySet> Setters { get; } = new();

  /// <summary>
  ///   Transform an `Expression{Func{T, TProperty}}` into an `Expression{Func{T, object?}}`.
  ///   Ie: remove the type information about the property
  /// </summary>
  /// <param name="property">Property definition</param>
  /// <typeparam name="TProperty">Type of the property</typeparam>
  /// <returns>An expression without the property type information</returns>
  private static Expression<Func<T, object?>> RemovePropertyType<TProperty>(Expression<Func<T, TProperty>> property)
  {
    Expression converted = Expression.Convert(property.Body,
                                              typeof(object));
    return Expression.Lambda<Func<T, object?>>(converted,
                                               property.Parameters);
  }

  /// <summary>
  ///   Add a new setter to the `UpdateDefinition`
  /// </summary>
  /// <param name="property">Property definition</param>
  /// <param name="value">New value</param>
  /// <typeparam name="TProperty">Type of the property</typeparam>
  /// <returns>The extended `UpdateDefinition`</returns>
  public UpdateDefinition<T> Set<TProperty>(Expression<Func<T, TProperty>> property,
                                            TProperty                      value)
  {
    Setters.Add(new PropertySet
                {
                  Value    = value,
                  Property = RemovePropertyType(property),
                });
    return this;
  }

  /// <summary>
  ///   Apply all the updates to <paramref name="x" />
  /// </summary>
  /// <param name="x">Object to update</param>
  public void ApplyTo(T x)
  {
    foreach (var setter in Setters)
    {
      setter.ApplyTo(x);
    }
  }

  /// <summary>
  ///   Express the update of a property of an object.
  /// </summary>
  [LogAsScalar]
  public record PropertySet
  {
    /// <summary>
    ///   Property definition
    /// </summary>
    [LogAsScalar]
    public required Expression<Func<T, object?>> Property { get; init; }

    /// <summary>
    ///   New value
    /// </summary>
    public required object? Value { get; init; }

    /// <summary>
    ///   Set the property from the object <paramref name="x" /> to the specified value
    /// </summary>
    /// <param name="x">Object to update</param>
    public void ApplyTo(T x)
      => typeof(T).GetProperty(Property.GetMember()
                                       .Name)!.SetValue(x,
                                                        Value);

    /// <summary>
    ///   Deconstruct a <see cref="Property" /> in its components
    /// </summary>
    /// <param name="selector">Property definition</param>
    /// <param name="newValue">New value</param>
    public void Deconstruct(out Expression<Func<T, object?>> selector,
                            out object?                      newValue)
    {
      selector = Property;
      newValue = Value;
    }
  }
}
