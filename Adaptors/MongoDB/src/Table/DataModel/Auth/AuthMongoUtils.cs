// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
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
using System.Text;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Auth;

public static class AuthMongoUtils
{
  public static string ToOidString(this string value)
    => IdSerializer.ToValidIdString(value);
}

public class IdSerializer : SerializerBase<string>
{
  public static readonly IdSerializer Instance = new();

  public override string Deserialize(BsonDeserializationContext context,
                                     BsonDeserializationArgs    args)
    => Deserialize(context.Reader.ReadObjectId());

  public string Deserialize(ObjectId id)
    => id.ToString();

  public ObjectId Serialize(string value)
    => ObjectId.Parse(value);

  public override void Serialize(BsonSerializationContext context,
                                 BsonSerializationArgs    args,
                                 string                   value)
    => context.Writer.WriteObjectId(Serialize(value));

  public static string ToValidIdString(string value)
    => ObjectId.Parse(Convert.ToHexString(Encoding.Default.GetBytes((value.Length <= 12
                                                                       ? value
                                                                       : value.GetHashCode()
                                                                              .ToString()).PadLeft(12,
                                                                                                   '='))))
               .ToString();
}

public class IdArraySerializer : SerializerBase<string[]>
{
  public static readonly IdArraySerializer Instance = new();

  public override string[] Deserialize(BsonDeserializationContext context,
                                       BsonDeserializationArgs    args)
  {
    var res = new List<string>();
    context.Reader.ReadStartArray();
    while (context.Reader.ReadBsonType() != BsonType.EndOfDocument)
    {
      res.Add(IdSerializer.Instance.Deserialize(context.Reader.ReadObjectId()));
    }

    context.Reader.ReadEndArray();
    return res.ToArray();
  }

  public override void Serialize(BsonSerializationContext context,
                                 BsonSerializationArgs    args,
                                 string[]                 value)
  {
    context.Writer.WriteStartArray();
    foreach (var s in value)
    {
      context.Writer.WriteObjectId(IdSerializer.Instance.Serialize(s));
    }

    context.Writer.WriteEndArray();
  }
}
