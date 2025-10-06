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
using System.Linq;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Auth;

/// <summary>
///   Extension class for MongoDB string conversion
/// </summary>
public static class AuthMongoUtils
{
  /// <summary>
  ///   Extension method to transform a string to an objectId
  /// </summary>
  /// <param name="value">The string to convert</param>
  /// <returns>Converted string</returns>
  public static string ToOidString(this string value)
    => IdSerializer.ToValidIdString(value);
}

/// <summary>
///   Serializer class to/from Object to/from string
/// </summary>
public class IdSerializer : SerializerBase<string>
{
  /// <summary>
  ///   Singleton instance of the serializer
  /// </summary>
  public static readonly IdSerializer Instance = new();

  /// <summary>
  ///   Method used by the MongoDB driver to deserialize an ObjectID to string
  /// </summary>
  /// <param name="context">Deserialization context</param>
  /// <param name="args">Deserialization arguments</param>
  /// <returns>ObjectID as a string</returns>
  public override string Deserialize(BsonDeserializationContext context,
                                     BsonDeserializationArgs    args)
    => Deserialize(context.Reader.ReadObjectId());

  /// <summary>
  ///   Method to deserialize an objectId into a string
  /// </summary>
  /// <param name="id">The ObjectId</param>
  /// <returns>ObjectId as a string</returns>
  public static string Deserialize(ObjectId id)
    => id.ToString();

  /// <summary>
  ///   Method to serialize an string into an ObjectId
  /// </summary>
  /// <param name="value">the string, must be a 24 length hexstring</param>
  /// <returns>ObjectId</returns>
  public static ObjectId Serialize(string value)
    => ObjectId.Parse(value);

  /// <summary>
  ///   Method used by the MongoDB driver to serialize a string to an ObjectID
  /// </summary>
  /// <param name="context">Serialization context</param>
  /// <param name="args">Serialization arguments</param>
  /// <param name="value">String to serialize</param>
  public override void Serialize(BsonSerializationContext context,
                                 BsonSerializationArgs    args,
                                 string                   value)
    => context.Writer.WriteObjectId(Serialize(value));

  /// <summary>
  ///   Converts any string to a valid hex string to be used as a MongoDB ID
  /// </summary>
  /// <param name="value">the string to convert</param>
  /// <returns>String as an ObjectId string</returns>
  public static string ToValidIdString(string value)
  {
    var hash = value.GetHashCode();
    return ObjectId.Parse(Convert.ToHexString(new List<int>
                                              {
                                                hash,
                                                hash,
                                                hash,
                                              }.SelectMany(BitConverter.GetBytes)
                                               .ToArray()))
                   .ToString();
  }
}

/// <summary>
///   Serializer to handle arrays of strings and arrays of ObjectIds
/// </summary>
public class IdArraySerializer : SerializerBase<string[]>
{
  /// <summary>
  ///   Serializer singleton instance
  /// </summary>
  public static readonly IdArraySerializer Instance = new();

  /// <summary>
  ///   Method used by the MongoDB driver to deserialize an array of ObjectIDs to an array of strings
  /// </summary>
  /// <param name="context">Deserialization context</param>
  /// <param name="args">Deserialization arguments</param>
  /// <returns>Array of strings deserialized from ObjectIds</returns>
  public override string[] Deserialize(BsonDeserializationContext context,
                                       BsonDeserializationArgs    args)
  {
    var res = new List<string>();
    context.Reader.ReadStartArray();
    while (context.Reader.ReadBsonType() != BsonType.EndOfDocument)
    {
      res.Add(IdSerializer.Deserialize(context.Reader.ReadObjectId()));
    }

    context.Reader.ReadEndArray();
    return res.ToArray();
  }

  /// <summary>
  ///   Method used by the MongoDB driver to serialize an array of strings to an array of ObjectIDs
  /// </summary>
  /// <param name="context">Serialization context</param>
  /// <param name="args">Serialization arguments</param>
  /// <param name="value">Array of strings String to serialize</param>
  public override void Serialize(BsonSerializationContext context,
                                 BsonSerializationArgs    args,
                                 string[]                 value)
  {
    context.Writer.WriteStartArray();
    foreach (var s in value)
    {
      context.Writer.WriteObjectId(IdSerializer.Serialize(s));
    }

    context.Writer.WriteEndArray();
  }
}
