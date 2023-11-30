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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.util.resourceToString

class LogicalViewSuite extends BenchmarkQueryTest {

  override def beforeAll: Unit = {
    super.beforeAll
    sql(
      """
        |CREATE TABLE `lineorder` (`lo_orderkey` INT, `lo_linenumber` INT, `lo_custkey` INT,
        |`lo_partkey` INT, `lo_suppkey` INT, `lo_orderdate` INT, `lo_orderpriority` STRING,
        |`lo_shippriority` STRING, `lo_quantity` INT, `lo_extendedprice` INT,
        |`lo_ordertotalprice` INT, `lo_discount` INT, `lo_revenue` INT, `lo_supplycost` INT,
        |`lo_tax` INT, `lo_commitdate` INT, `lo_shipmode` STRING)
        |USING parquet
      """.stripMargin)
  }

  val logicalViewQueries: Seq[String] = Seq("1.1", "1.2")
  logicalViewQueries.foreach { name =>
    val queryString = resourceToString(s"logical-view/$name.sql",
      classLoader = Thread.currentThread.getContextClassLoader)
    test(name) {
      withSQLConf("spark.sql.legacy.storeAnalyzedPlanForView" -> String.valueOf(name.eq("1.2"))) {
        sql(queryString)
        val logical_v = spark.sessionState.catalog.globalTempViewManager
          .get("logical_v_" + name.replace(".", "_"))
        val partitionColumnNames = logical_v.get.tableMeta.partitionColumnNames
        assert(partitionColumnNames.size == 1)
        assert(partitionColumnNames.contains("lo_shipmode"))
      }
    }
  }
}
