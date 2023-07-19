// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

using ArmoniK.Api.gRPC.V1.Applications;

using Armonik.Api.Grpc.V1.Partitions;

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;

using Armonik.Api.gRPC.V1.Tasks;

using FilterField = Armonik.Api.gRPC.V1.Tasks.FilterField;

namespace ArmoniK.Core.Common.Tests.Helpers;

public static class FilterFieldExt
{
  private static string ToDisplay(this TaskField field)
  {
    switch (field.FieldCase)
    {
      case TaskField.FieldOneofCase.TaskSummaryField:
        return field.FieldCase + " " + field.TaskSummaryField.Field;
      case TaskField.FieldOneofCase.TaskOptionField:
        return field.FieldCase + " " + field.TaskOptionField.Field;
      case TaskField.FieldOneofCase.TaskOptionGenericField:
        return field.FieldCase + " " + field.TaskOptionGenericField.Field;
      case TaskField.FieldOneofCase.None:
      default:
        throw new ArgumentOutOfRangeException();
    }
  }

  private static string ToDisplay(this ApplicationField field)
  {
    switch (field.FieldCase)
    {
      case ApplicationField.FieldOneofCase.ApplicationField_:
        return field.FieldCase + " " + field.ApplicationField_.Field;
      case ApplicationField.FieldOneofCase.None:
      default:
        throw new ArgumentOutOfRangeException();
    }
  }

  private static string ToDisplay(this PartitionField field)
  {
    switch (field.FieldCase)
    {
      case PartitionField.FieldOneofCase.PartitionRawField:
        return field.FieldCase + " " + field.PartitionRawField.Field;
      case PartitionField.FieldOneofCase.None:
      default:
        throw new ArgumentOutOfRangeException();
    }
  }

  private static string ToDisplay(this ResultField field)
  {
    switch (field.FieldCase)
    {
      case ResultField.FieldOneofCase.ResultRawField:
        return field.FieldCase + " " + field.ResultRawField.Field;
      case ResultField.FieldOneofCase.None:
      default:
        throw new ArgumentOutOfRangeException();
    }
  }

  private static string ToDisplay(this SessionField field)
  {
    switch (field.FieldCase)
    {
      case SessionField.FieldOneofCase.SessionRawField:
        return field.FieldCase + " " + field.SessionRawField.Field;
      case SessionField.FieldOneofCase.TaskOptionField:
        return field.FieldCase + " " + field.TaskOptionField.Field;
      case SessionField.FieldOneofCase.TaskOptionGenericField:
        return field.FieldCase + " " + field.TaskOptionGenericField.Field;
      case SessionField.FieldOneofCase.None:
      default:
        throw new ArgumentOutOfRangeException();
    }
  }

  public static string ToDisplay(this FilterField filterField)
  {
    switch (filterField.FilterCase)
    {
      case FilterField.FilterOneofCase.Status:
      case FilterField.FilterOneofCase.Boolean:
      case FilterField.FilterOneofCase.Number:
      case FilterField.FilterOneofCase.String:
        return filterField.ToString();
      case FilterField.FilterOneofCase.Date:
        return filterField.FilterCase + " " + filterField.Date.Field.ToDisplay();
      case FilterField.FilterOneofCase.Array:
        return filterField.ToString();
      case FilterField.FilterOneofCase.None:
      default:
        throw new ArgumentOutOfRangeException();
    }
  }

  public static string ToDisplay(this Api.gRPC.V1.Applications.FilterField filterField)
  {
    switch (filterField.FilterCase)
    {
      case Api.gRPC.V1.Applications.FilterField.FilterOneofCase.Boolean:
      case Api.gRPC.V1.Applications.FilterField.FilterOneofCase.Number:
      case Api.gRPC.V1.Applications.FilterField.FilterOneofCase.String:
        return filterField.ToString();
      case Api.gRPC.V1.Applications.FilterField.FilterOneofCase.Date:
        return filterField.FilterCase + " " + filterField.Date.Field.ToDisplay();
      case Api.gRPC.V1.Applications.FilterField.FilterOneofCase.Array:
        return filterField.ToString();
      case Api.gRPC.V1.Applications.FilterField.FilterOneofCase.None:
      default:
        throw new ArgumentOutOfRangeException();
    }
  }

  public static string ToDisplay(this Armonik.Api.Grpc.V1.Partitions.FilterField filterField)
  {
    switch (filterField.FilterCase)
    {
      case Armonik.Api.Grpc.V1.Partitions.FilterField.FilterOneofCase.Boolean:
      case Armonik.Api.Grpc.V1.Partitions.FilterField.FilterOneofCase.Number:
      case Armonik.Api.Grpc.V1.Partitions.FilterField.FilterOneofCase.String:
        return filterField.ToString();
      case Armonik.Api.Grpc.V1.Partitions.FilterField.FilterOneofCase.Date:
        return filterField.FilterCase + " " + filterField.Date.Field.ToDisplay();
      case Armonik.Api.Grpc.V1.Partitions.FilterField.FilterOneofCase.Array:
        return filterField.ToString();
      case Armonik.Api.Grpc.V1.Partitions.FilterField.FilterOneofCase.None:
      default:
        throw new ArgumentOutOfRangeException();
    }
  }

  public static string ToDisplay(this Api.gRPC.V1.Results.FilterField filterField)
  {
    switch (filterField.FilterCase)
    {
      case Api.gRPC.V1.Results.FilterField.FilterOneofCase.Status:
      case Api.gRPC.V1.Results.FilterField.FilterOneofCase.Boolean:
      case Api.gRPC.V1.Results.FilterField.FilterOneofCase.Number:
      case Api.gRPC.V1.Results.FilterField.FilterOneofCase.String:
        return filterField.ToString();
      case Api.gRPC.V1.Results.FilterField.FilterOneofCase.Date:
        return filterField.FilterCase + " " + filterField.Date.Field.ToDisplay();
      case Api.gRPC.V1.Results.FilterField.FilterOneofCase.Array:
        return filterField.ToString();
      case Api.gRPC.V1.Results.FilterField.FilterOneofCase.None:
      default:
        throw new ArgumentOutOfRangeException();
    }
  }

  public static string ToDisplay(this Api.gRPC.V1.Sessions.FilterField filterField)
  {
    switch (filterField.FilterCase)
    {
      case Api.gRPC.V1.Sessions.FilterField.FilterOneofCase.Status:
      case Api.gRPC.V1.Sessions.FilterField.FilterOneofCase.Boolean:
      case Api.gRPC.V1.Sessions.FilterField.FilterOneofCase.Number:
      case Api.gRPC.V1.Sessions.FilterField.FilterOneofCase.String:
        return filterField.ToString();
      case Api.gRPC.V1.Sessions.FilterField.FilterOneofCase.Date:
        return filterField.FilterCase + " " + filterField.Date.Field.ToDisplay();
      case Api.gRPC.V1.Sessions.FilterField.FilterOneofCase.Array:
        return filterField.ToString();
      case Api.gRPC.V1.Sessions.FilterField.FilterOneofCase.None:
      default:
        throw new ArgumentOutOfRangeException();
    }
  }
}
