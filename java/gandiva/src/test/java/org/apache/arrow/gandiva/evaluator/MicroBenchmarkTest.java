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

package org.apache.arrow.gandiva.evaluator;

import java.util.List;

import org.apache.arrow.gandiva.expression.Condition;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

public class MicroBenchmarkTest extends BaseEvaluatorTest {

  private double toleranceRatio = 4.0;

  @Test
  public void testAdd10() throws Exception {
    Field x = Field.nullable("x", int32);
    Field n2x = Field.nullable("n2x", int32);
    Field n3x = Field.nullable("n3x", int32);
    Field n4x = Field.nullable("n4x", int32);
    Field n5x = Field.nullable("n5x", int32);
    Field n6x = Field.nullable("n6x", int32);
    Field n7x = Field.nullable("n7x", int32);
    Field n8x = Field.nullable("n8x", int32);
    Field n9x = Field.nullable("n9x", int32);
    Field n10x = Field.nullable("n10x", int32);

    // x + n2x + n3x
    TreeNode add =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(TreeBuilder.makeField(x), TreeBuilder.makeField(n2x)), int32);
    TreeNode add1 =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(add, TreeBuilder.makeField(n3x)), int32);
    TreeNode add2 =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(add1, TreeBuilder.makeField(n4x)), int32);
    TreeNode add3 =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(add2, TreeBuilder.makeField(n5x)), int32);
    TreeNode add4 =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(add3, TreeBuilder.makeField(n6x)), int32);
    TreeNode add5 =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(add4, TreeBuilder.makeField(n7x)), int32);
    TreeNode add6 =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(add5, TreeBuilder.makeField(n8x)), int32);
    TreeNode add7 =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(add6, TreeBuilder.makeField(n9x)), int32);
    TreeNode add8 =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(add7, TreeBuilder.makeField(n10x)), int32);
    ExpressionTree expr = TreeBuilder.makeExpression(add8, x);

    List<Field> cols = Lists.newArrayList(x, n2x, n3x, n4x, n5x, n6x, n7x, n8x, n9x, n10x);
    Schema schema = new Schema(cols);

    long timeTaken = timedProject(new Int32DataAndVectorGenerator(allocator),
        schema,
        Lists.newArrayList(expr),
        200 * MILLION, 16 * THOUSAND,
        4);
    System.out.println("Time taken for projecting 200m records of add10 is " + timeTaken + "ms");
  }

  @Test
  public void testAdd10_Float() throws Exception {
    Field x = Field.nullable("x", float64);
    Field n2x = Field.nullable("n2x", float64);
    Field n3x = Field.nullable("n3x", float64);
    Field n4x = Field.nullable("n4x", float64);
    Field n5x = Field.nullable("n5x", float64);
    Field n6x = Field.nullable("n6x", float64);
    Field n7x = Field.nullable("n7x", float64);
    Field n8x = Field.nullable("n8x", float64);
    Field n9x = Field.nullable("n9x", float64);
    Field n10x = Field.nullable("n10x", float64);

    // x + n2x + n3x
    TreeNode add =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(TreeBuilder.makeField(x), TreeBuilder.makeField(n2x)), float64);
    TreeNode add1 =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(add, TreeBuilder.makeField(n3x)), float64);
    TreeNode add2 =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(add1, TreeBuilder.makeField(n4x)), float64);
    TreeNode add3 =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(add2, TreeBuilder.makeField(n5x)), float64);
    TreeNode add4 =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(add3, TreeBuilder.makeField(n6x)), float64);
    TreeNode add5 =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(add4, TreeBuilder.makeField(n7x)), float64);
    TreeNode add6 =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(add5, TreeBuilder.makeField(n8x)), float64);
    TreeNode add7 =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(add6, TreeBuilder.makeField(n9x)), float64);
    TreeNode add8 =
        TreeBuilder.makeFunction(
            "add", Lists.newArrayList(add7, TreeBuilder.makeField(n10x)), float64);
    ExpressionTree expr = TreeBuilder.makeExpression(add8, x);

    List<Field> cols = Lists.newArrayList(x, n2x, n3x, n4x, n5x, n6x, n7x, n8x, n9x, n10x);
    Schema schema = new Schema(cols);

    long timeTaken = timedProject(new Int32DataAndVectorGenerator(allocator),
        schema,
        Lists.newArrayList(expr),
        200 * MILLION, 16 * THOUSAND,
        4);
    System.out.println("Time taken for projecting 200m records of add10 is " + timeTaken + "ms");
  }
  /*
  @Test
  public void testIf() throws Exception {
     * when x < 10 then 0
     * when x < 20 then 1
     * when x < 30 then 2
     * when x < 40 then 3
     * when x < 50 then 4
     * when x < 60 then 5
     * when x < 70 then 6
     * when x < 80 then 7
     * when x < 90 then 8
     * when x < 100 then 9
     * when x < 110 then 10
     * when x < 120 then 11
     * when x < 130 then 12
     * when x < 140 then 13
     * when x < 150 then 14
     * when x < 160 then 15
     * when x < 170 then 16
     * when x < 180 then 17
     * when x < 190 then 18
     * when x < 200 then 19
     * else 20
    Field x = Field.nullable("x", int32);
    TreeNode xNode = TreeBuilder.makeField(x);

    // if (x < 100) then 9 else 10
    int returnValue = 20;
    TreeNode topNode = TreeBuilder.makeLiteral(returnValue);
    int compareWith = 200;
    while (compareWith >= 10) {
      // cond (x < compareWith)
      TreeNode condNode =
          TreeBuilder.makeFunction(
              "less_than",
              Lists.newArrayList(xNode, TreeBuilder.makeLiteral(compareWith)),
              boolType);
      topNode =
          TreeBuilder.makeIf(
              condNode, // cond (x < compareWith)
              TreeBuilder.makeLiteral(returnValue), // then returnValue
              topNode, // else topNode
              int32);
      compareWith -= 10;
      returnValue--;
    }

    ExpressionTree expr = TreeBuilder.makeExpression(topNode, x);
    Schema schema = new Schema(Lists.newArrayList(x));

    long timeTaken = timedProject(new BoundedInt32DataAndVectorGenerator(allocator, 250),
        schema,
        Lists.newArrayList(expr),
        1 * MILLION, 16 * THOUSAND,
        4);
    System.out.println("Time taken for projecting 10m records of nestedIf is " + timeTaken + "ms");
    Assert.assertTrue(timeTaken <= 15 * toleranceRatio);
  }

  @Test
  public void testFilterAdd2() throws Exception {
    Field x = Field.nullable("x", int32);
    Field n2x = Field.nullable("n2x", int32);
    Field n3x = Field.nullable("n3x", int32);

    // x + n2x < n3x
    TreeNode add = TreeBuilder.makeFunction("add",
        Lists.newArrayList(TreeBuilder.makeField(x), TreeBuilder.makeField(n2x)), int32);
    TreeNode lessThan = TreeBuilder
        .makeFunction("less_than", Lists.newArrayList(add, TreeBuilder.makeField(n3x)), boolType);
    Condition condition = TreeBuilder.makeCondition(lessThan);

    List<Field> cols = Lists.newArrayList(x, n2x, n3x);
    Schema schema = new Schema(cols);

    long timeTaken = timedFilter(new Int32DataAndVectorGenerator(allocator),
        schema,
        condition,
        1 * MILLION, 16 * THOUSAND,
        4);
    System.out.println("Time taken for filtering 10m records of a+b<c is " + timeTaken + "ms");
    Assert.assertTrue(timeTaken <= 12 * toleranceRatio);
  }
  */


}
