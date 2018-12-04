/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.expressions

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.ScalaUDF
import org.apache.spark.sql.types.DataType

/**
 * A user-defined function. To create one, use the `udf` functions in `functions`.
 *
 * As an example:
 * {{{
 *   // Define a UDF that returns true or false based on some numeric score.
 *   val predict = udf((score: Double) => score > 0.5)
 *
 *   // Projects a column that adds a prediction column based on the score column.
 *   df.select( predict(df("score")) )
 * }}}
 *
 * @since 1.3.0
 */
@Stable
sealed trait UserDefinedFunction {

  /**
   * Returns true when the UDF can return a nullable value.
   *
   * @since 2.3.0
   */
  def nullable: Boolean

  /**
   * Returns true iff the UDF is deterministic, i.e. the UDF produces the same output given the same
   * input.
   *
   * @since 2.3.0
   */
  def deterministic: Boolean

  /**
   * Returns an expression that invokes the UDF, using the given arguments.
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def apply(exprs: Column*): Column

  /**
   * Updates UserDefinedFunction with a given name.
   *
   * @since 2.3.0
   */
  def withName(name: String): UserDefinedFunction

  /**
   * Updates UserDefinedFunction to non-nullable.
   *
   * @since 2.3.0
   */
  def asNonNullable(): UserDefinedFunction

  /**
   * Updates UserDefinedFunction to nondeterministic.
   *
   * @since 2.3.0
   */
  def asNondeterministic(): UserDefinedFunction
}

private[sql] case class SparkUserDefinedFunction(
    f: AnyRef,
    dataType: DataType,
    inputTypes: Option[Seq[DataType]],
    nullableTypes: Option[Seq[Boolean]],
    name: Option[String] = None,
    nullable: Boolean = true,
    deterministic: Boolean = true) extends UserDefinedFunction {

  @scala.annotation.varargs
  override def apply(exprs: Column*): Column = {
    // TODO: make sure this class is only instantiated through `SparkUserDefinedFunction.create()`
    // and `nullableTypes` is always set.
    if (inputTypes.isDefined) {
      assert(inputTypes.get.length == nullableTypes.get.length)
    }

    val inputsNullSafe = nullableTypes.getOrElse {
      ScalaReflection.getParameterTypeNullability(f)
    }

    Column(ScalaUDF(
      f,
      dataType,
      exprs.map(_.expr),
      inputsNullSafe,
      inputTypes.getOrElse(Nil),
      udfName = name,
      nullable = nullable,
      udfDeterministic = deterministic))
  }

  override def withName(name: String): UserDefinedFunction = {
    copy(name = Option(name))
  }

  override def asNonNullable(): UserDefinedFunction = {
    if (!nullable) {
      this
    } else {
      copy(nullable = false)
    }
  }

  override def asNondeterministic(): UserDefinedFunction = {
    if (!deterministic) {
      this
    } else {
      copy(deterministic = false)
    }
  }
}

private[sql] object SparkUserDefinedFunction {

  def create(
      f: AnyRef,
      dataType: DataType,
      inputSchemas: Seq[Option[ScalaReflection.Schema]]): UserDefinedFunction = {
    val inputTypes = if (inputSchemas.contains(None)) {
      None
    } else {
      Some(inputSchemas.map(_.get.dataType))
    }
    val nullableTypes = Some(inputSchemas.map(_.map(_.nullable).getOrElse(true)))
    SparkUserDefinedFunction(f, dataType, inputTypes, nullableTypes)
  }
}