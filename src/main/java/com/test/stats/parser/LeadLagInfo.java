/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.test.stats.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

/**
 * Lead Lag functionality
 */
public class LeadLagInfo {

  /**
   * list of LL invocations in a Query.
   */
  List<ExprNodeGenericFuncDesc> leadLagExprs;

  /**
   * map from the Select Expr Node to the LL Function invocations in it.
   */
  Map<ExprNodeDesc, List<ExprNodeGenericFuncDesc>> mapTopExprToLLFunExprs;

  private void addLeadLagExpr(ExprNodeGenericFuncDesc llFunc) {
    leadLagExprs = leadLagExprs == null ? new ArrayList<ExprNodeGenericFuncDesc>() : leadLagExprs;
    leadLagExprs.add(llFunc);
  }

  public List<ExprNodeGenericFuncDesc> getLeadLagExprs() {
    return leadLagExprs;
  }

  public void addLLFuncExprForTopExpr(ExprNodeDesc topExpr, ExprNodeGenericFuncDesc llFuncExpr) {
    addLeadLagExpr(llFuncExpr);
    mapTopExprToLLFunExprs = mapTopExprToLLFunExprs == null ?
        new HashMap<ExprNodeDesc, List<ExprNodeGenericFuncDesc>>() : mapTopExprToLLFunExprs;
    List<ExprNodeGenericFuncDesc> funcList = mapTopExprToLLFunExprs.get(topExpr);
    if (funcList == null) {
      funcList = new ArrayList<ExprNodeGenericFuncDesc>();
      mapTopExprToLLFunExprs.put(topExpr, funcList);
    }
    funcList.add(llFuncExpr);
  }

  public List<ExprNodeGenericFuncDesc> getLLFuncExprsInTopExpr(ExprNodeDesc topExpr) {
    if (mapTopExprToLLFunExprs == null) {
      return null;
    }
    return mapTopExprToLLFunExprs.get(topExpr);
  }
}