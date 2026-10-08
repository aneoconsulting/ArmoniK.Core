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

using System.Collections.Generic;
using System.Linq;

namespace ArmoniK.Core.Authorization;

/// <summary>
///   Strict parser of the condition grammar of the access model
/// </summary>
/// <remarks>
///   Only the six forms of the access model are accepted, joined by <c>&amp;&amp;</c>. Types are checked and never
///   coerced: a comparison between a value and a list is refused rather than evaluated to false. Literals are
///   double quoted, cannot be empty and cannot contain a double quote, a backslash or a control character.
/// </remarks>
internal static class ConditionParser
{
  private const string SessionOwner        = "session.owner";
  private const string SessionCreatedUnder = "session.createdUnder";
  private const string SessionSharedWith   = "session.sharedWith";
  private const string SessionDelegatedTo  = "session.delegatedTo";
  private const string RequestCaller       = "request.caller";
  private const string RequestPartition    = "request.partition";
  private const string GrantPrincipal      = "grant.principal";

  private const string SupportedForms = "session.owner == request.caller, \"P\" in session.createdUnder, grant.principal in session.createdUnder, " +
                                        "request.caller in session.sharedWith, request.caller in session.delegatedTo, request.partition == \"Q\"";

  private static readonly Dictionary<string, bool> AttributeIsList = new()
                                                                     {
                                                                       [SessionOwner]        = false,
                                                                       [SessionCreatedUnder] = true,
                                                                       [SessionSharedWith]   = true,
                                                                       [SessionDelegatedTo]  = true,
                                                                       [RequestCaller]       = false,
                                                                       [RequestPartition]    = false,
                                                                       [GrantPrincipal]      = false,
                                                                     };

  public static Condition Parse(string text)
  {
    var tokens = Tokenize(text);

    if (tokens.Count == 0)
    {
      throw new InvalidConditionException("A condition cannot be empty: omit it to write a grant without condition");
    }

    var terms = new List<Term>();
    var i     = 0;

    while (true)
    {
      if (i + 3 > tokens.Count)
      {
        throw new InvalidConditionException($"Incomplete term at the end of '{text}'");
      }

      terms.Add(ParseTerm(tokens[i],
                          tokens[i + 1],
                          tokens[i + 2]));
      i += 3;

      if (i == tokens.Count)
      {
        break;
      }

      if (tokens[i].Kind != TokenKind.And)
      {
        throw new InvalidConditionException($"Expected '&&' after '{terms[^1]}', found '{tokens[i].Text}'");
      }

      i++;
    }

    return new Condition(terms);
  }

  private static Term ParseTerm(Token left,
                                Token op,
                                Token right)
  {
    if (op.Kind is not (TokenKind.Equal or TokenKind.In))
    {
      throw new InvalidConditionException($"Expected '==' or 'in' after '{left.Text}', found '{op.Text}'");
    }

    var leftIsList  = OperandIsList(left);
    var rightIsList = OperandIsList(right);

    if (op.Kind == TokenKind.Equal && (leftIsList || rightIsList))
    {
      throw new InvalidConditionException($"'{left.Text} == {right.Text}' compares a value with a list: use 'in' to test membership");
    }

    if (op.Kind == TokenKind.In && (leftIsList || !rightIsList))
    {
      throw new InvalidConditionException($"'{left.Text} in {right.Text}' needs a value on the left and a list on the right");
    }

    return (left.Kind, left.Text, op.Kind, right.Kind, right.Text) switch
           {
             (TokenKind.Identifier, SessionOwner, TokenKind.Equal, TokenKind.Identifier, RequestCaller)      => new OwnerIsCaller(),
             (TokenKind.String, var p, TokenKind.In, TokenKind.Identifier, SessionCreatedUnder)              => new LiteralInCreatedUnder(p),
             (TokenKind.Identifier, GrantPrincipal, TokenKind.In, TokenKind.Identifier, SessionCreatedUnder) => new GrantPrincipalInCreatedUnder(),
             (TokenKind.Identifier, RequestCaller, TokenKind.In, TokenKind.Identifier, SessionSharedWith)    => new CallerInSharedWith(),
             (TokenKind.Identifier, RequestCaller, TokenKind.In, TokenKind.Identifier, SessionDelegatedTo)   => new CallerInDelegatedTo(),
             (TokenKind.Identifier, RequestPartition, TokenKind.Equal, TokenKind.String, var q)              => new PartitionIs(q),
             _ => throw new InvalidConditionException($"'{Display(left)} {op.Text} {Display(right)}' is not a supported form; supported forms are: {SupportedForms}"),
           };
  }

  private static bool OperandIsList(Token token)
    => token.Kind switch
       {
         TokenKind.String => false,
         TokenKind.Identifier => AttributeIsList.TryGetValue(token.Text,
                                                             out var isList)
                                   ? isList
                                   : throw new InvalidConditionException($"Unknown attribute '{token.Text}'; known attributes are: {string.Join(", ", AttributeIsList.Keys)}"),
         _ => throw new InvalidConditionException($"Expected an attribute or a quoted value, found '{token.Text}'"),
       };

  private static string Display(Token token)
    => token.Kind == TokenKind.String
         ? $"\"{token.Text}\""
         : token.Text;

  private static List<Token> Tokenize(string text)
  {
    var tokens = new List<Token>();
    var i      = 0;

    while (i < text.Length)
    {
      var c = text[i];

      if (c is ' ' or '\t' or '\n' or '\r')
      {
        i++;
        continue;
      }

      if (c == '"')
      {
        var end = text.IndexOf('"',
                               i + 1);
        if (end < 0)
        {
          throw new InvalidConditionException($"Unterminated quoted value in '{text}'");
        }

        var value = text[(i + 1)..end];
        if (value.Length == 0)
        {
          throw new InvalidConditionException("A quoted value cannot be empty");
        }

        if (value.Any(ch => ch == '\\' || char.IsControl(ch)))
        {
          throw new InvalidConditionException($"The quoted value \"{value}\" contains a backslash or a control character");
        }

        tokens.Add(new Token(TokenKind.String,
                             value));
        i = end + 1;
        continue;
      }

      if (c == '=' && i + 1 < text.Length && text[i + 1] == '=')
      {
        tokens.Add(new Token(TokenKind.Equal,
                             "=="));
        i += 2;
        continue;
      }

      if (c == '&' && i + 1 < text.Length && text[i + 1] == '&')
      {
        tokens.Add(new Token(TokenKind.And,
                             "&&"));
        i += 2;
        continue;
      }

      if (char.IsAsciiLetter(c))
      {
        var start = i;
        while (i < text.Length && (char.IsAsciiLetter(text[i]) || text[i] == '.'))
        {
          i++;
        }

        var word = text[start..i];
        tokens.Add(word == "in"
                     ? new Token(TokenKind.In,
                                 word)
                     : new Token(TokenKind.Identifier,
                                 word));
        continue;
      }

      throw new InvalidConditionException($"Unexpected '{c}' at position {i} in '{text}'; terms can only be joined with '&&'");
    }

    return tokens;
  }

  private enum TokenKind
  {
    Identifier,
    String,
    Equal,
    In,
    And,
  }

  private sealed record Token(TokenKind Kind,
                              string    Text);
}
