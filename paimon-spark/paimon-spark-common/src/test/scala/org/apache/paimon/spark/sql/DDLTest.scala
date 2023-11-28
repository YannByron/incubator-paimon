/*
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
package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonSparkTestBase

import org.junit.jupiter.api.Assertions

class DDLTest extends PaimonSparkTestBase {

  import testImplicits._

  test("xxx") {
    Seq((1L, "x1", "2023"), (2L, "x2", "2023"))
      .toDF("a", "b", "pt")
      .createOrReplaceTempView("source")

    spark.sql("""
                |CREATE TABLE t1 (id Long, name STRING, pt STRING) partitioned by (pt)
                |""".stripMargin)

    spark.sql("""
                |INSERT INTO t1 VALUES (1L, "a", "2023"), (3L, "c", "2023"), (5, "e", "2025")
                |""".stripMargin)

    spark
      .sql("""
             |SELECT * FROM source join t1
             |ON source.pt = t1.pt and source.pt = '2023'
             |""".stripMargin)
      .explain(true)
  }

  test("xxx22") {
    Seq((1L, "x1", "2023"), (2L, "x2", "2023"), (3L, "x3", "2025"))
      .toDF("a", "b", "pt")
      .createOrReplaceTempView("source")

    spark.sql("""
                |CREATE TABLE t1 (id Long, name STRING, pt STRING) partitioned by (pt)
                |""".stripMargin)

    spark.sql(
      """
        |INSERT INTO t1 VALUES (1L, "a", "2023"), (3L, "c", "2023"), (5, "e", "2025"), (7, "g", "2027")
        |""".stripMargin)

//    spark
//      .sql("""
//             |SELECT * FROM source join t1
//             |ON source.pt = t1.pt and source.a >= 3
//             |where t1.id >= 3
//             |""".stripMargin)
//      .explain(true)

    spark
      .sql("""
             |SELECT * FROM source join t1
             |ON source.pt = t1.pt and source.a >= 3
             |where t1.id >= 3
             |""".stripMargin)
      .show
  }

  test("Paimon: Create Table As Select") {
    Seq((1L, "x1", "2023"), (2L, "x2", "2023"))
      .toDF("a", "b", "pt")
      .createOrReplaceTempView("source")

    spark.sql("""
                |CREATE TABLE t1 AS SELECT * FROM source
                |""".stripMargin)
    val t1 = loadTable("t1")
    Assertions.assertTrue(t1.primaryKeys().isEmpty)
    Assertions.assertTrue(t1.partitionKeys().isEmpty)

    spark.sql(
      """
        |CREATE TABLE t2
        |PARTITIONED BY (pt)
        |TBLPROPERTIES ('bucket' = '5', 'primary-key' = 'a,pt', 'target-file-size' = '128MB')
        |AS SELECT * FROM source
        |""".stripMargin)
    val t2 = loadTable("t2")
    Assertions.assertEquals(2, t2.primaryKeys().size())
    Assertions.assertTrue(t2.primaryKeys().contains("a"))
    Assertions.assertTrue(t2.primaryKeys().contains("pt"))
    Assertions.assertEquals(1, t2.partitionKeys().size())
    Assertions.assertEquals("pt", t2.partitionKeys().get(0))

    // check all the core options
    Assertions.assertEquals("5", t2.options().get("bucket"))
    Assertions.assertEquals("128MB", t2.options().get("target-file-size"))
  }

}
