using System;
using System.Linq.Expressions;
public static class ExpressionExt
{
  /// <summary>
  /// Combines two predicate expressions using a logical AND condition
  /// </summary>
  /// <typeparam name="T"> The type of the input parameter the predicate expressions are evaluated on </typeparam>
  /// <param name="expr1"> The first predicate expression to combine </param>
  /// <param name="expr2"> The second predicate expression to combine </param>
  /// <returns> A new predicate expression that represents the logical AND of the two expressions </returns>
  public static Expression<Func<T, bool>> ExpressionAnd<T>(this Expression<Func<T, bool>> expr1,
                                                              Expression<Func<T, bool>>   expr2)
    => Expression.Lambda<Func<T, bool>>(Expression.AndAlso(expr1.Body,
                                                           expr2.Body),
                                        expr2.Parameters);
  /// <summary>
  /// Combines two predicate expressions using a logical OR condition
  /// </summary>
  /// <typeparam name="T"> The type of the input parameter the predicate expressions are evaluated on </typeparam>
  /// <param name="expr1"> The first predicate expression to combine </param>
  /// <param name="expr2"> The second predicate expression to combine </param>
  /// <returns> A new predicate expression that represents the logical OR of the two expressions </returns>
  public static Expression<Func<T, bool>> ExpressionOr<T>(this Expression<Func<T, bool>> expr1,
                                                          Expression<Func<T, bool>> expr2)
    => Expression.Lambda<Func<T, bool>>(Expression.OrElse(expr1.Body,
                                                          expr2.Body),
                                        expr2.Parameters);
  }
