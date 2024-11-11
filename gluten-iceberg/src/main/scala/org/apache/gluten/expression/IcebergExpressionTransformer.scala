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
package org.apache.gluten.expression

import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.sql.catalyst.expressions.{Expression, IcebergBucketTransform}
import org.apache.spark.sql.types.IntegerType

import java.lang.{Long => JLong}
import java.util.{ArrayList => JArrayList, HashMap => JHashMap}

case class IcebergExpressionTransformer(
    substraitExprName: String,
    children: Seq[ExpressionTransformer],
    original: Expression)
  extends ExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val (firstArgValue, firstArgType) = original match {
      case IcebergBucketTransform(numBuckets, _) =>
        (numBuckets, IntegerType)
      case other =>
        throw new IllegalArgumentException(s"No need to transform for ${other.getClass.getName}")
    }

    val childrenTypes = firstArgType +: original.children.map(child => child.dataType)

    val functionMap = args.asInstanceOf[JHashMap[String, JLong]]
    val functionName =
      ConverterUtils.makeFuncName(substraitExprName, childrenTypes, FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)

    val nodes = new JArrayList[ExpressionNode]()
    nodes.add(ExpressionBuilder.makeIntLiteral(firstArgValue))
    children.foreach(
      expression => {
        nodes.add(expression.doTransform(args))
      })

    ExpressionBuilder.makeScalarFunction(functionId, nodes, typeNode)
  }
}
