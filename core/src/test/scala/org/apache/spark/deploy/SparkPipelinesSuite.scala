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

package org.apache.spark.deploy

import org.apache.spark.SparkUserAppException

class SparkPipelinesSuite extends SparkSubmitTestUtils {
  test("only spark submit args") {
    val args = Array(
      "--remote",
      "local[2]",
      "--supervise",
      "--conf",
      "spark.conf1=2",
      "--conf",
      "spark.conf2=3"
    )
    assert(
      SparkPipelines.constructSparkSubmitArgs(
        pipelinesCliFile = "abc/python/pyspark/pipelines/cli.py", args = args) ==
      Seq(
        "--supervise",
        "--conf",
        "spark.conf1=2",
        "--conf",
        "spark.conf2=3",
        "--conf",
        "spark.api.mode=connect",
        "--remote",
        "local[2]",
        "abc/python/pyspark/pipelines/cli.py"
      )
    )
  }

  test("only pipelines args") {
    val args = Array(
      "run",
      "--spec",
      "spark-pipeline.yml"
    )
    assert(
      SparkPipelines.constructSparkSubmitArgs(
        pipelinesCliFile = "abc/python/pyspark/pipelines/cli.py", args = args) ==
      Seq(
        "--conf",
        "spark.api.mode=connect",
        "--remote",
        "local",
        "abc/python/pyspark/pipelines/cli.py",
        "run",
        "--spec",
        "spark-pipeline.yml"
      )
    )
  }

  test("spark-submit and pipelines args") {
    val args = Array(
      "--remote",
      "local[2]",
      "run",
      "--supervise",
      "--spec",
      "spark-pipeline.yml",
      "--conf",
      "spark.conf2=3"
    )
    assert(
      SparkPipelines.constructSparkSubmitArgs(
        pipelinesCliFile = "abc/python/pyspark/pipelines/cli.py", args = args) ==
      Seq(
        "--supervise",
        "--conf",
        "spark.conf2=3",
        "--conf",
        "spark.api.mode=connect",
        "--remote",
        "local[2]",
        "abc/python/pyspark/pipelines/cli.py",
        "run",
        "--spec",
        "spark-pipeline.yml"
      )
    )
  }

  test("class arg prohibited") {
    val args = Array(
      "--class",
      "org.apache.spark.deploy.SparkPipelines"
    )
    intercept[SparkUserAppException] {
      SparkPipelines.constructSparkSubmitArgs(
        pipelinesCliFile = "abc/python/pyspark/pipelines/cli.py", args = args)
    }
  }

  test("spark.api.mode arg") {
    var args = Array("--conf", "spark.api.mode=classic")
    intercept[SparkUserAppException] {
      SparkPipelines.constructSparkSubmitArgs(
        pipelinesCliFile = "abc/python/pyspark/pipelines/cli.py", args = args)
    }
    args = Array("-c", "spark.api.mode=classic")
    intercept[SparkUserAppException] {
      SparkPipelines.constructSparkSubmitArgs(
        pipelinesCliFile = "abc/python/pyspark/pipelines/cli.py", args = args)
    }
    args = Array("--conf", "spark.api.mode=CONNECT")
    assert(
      SparkPipelines.constructSparkSubmitArgs(
        pipelinesCliFile = "abc/python/pyspark/pipelines/cli.py", args = args) ==
        Seq(
          "--conf",
          "spark.api.mode=connect",
          "--remote",
          "local",
          "abc/python/pyspark/pipelines/cli.py"
        )
    )
    args = Array("--conf", "spark.api.mode=CoNNect")
    assert(
      SparkPipelines.constructSparkSubmitArgs(
        pipelinesCliFile = "abc/python/pyspark/pipelines/cli.py", args = args) ==
        Seq(
          "--conf",
          "spark.api.mode=connect",
          "--remote",
          "local",
          "abc/python/pyspark/pipelines/cli.py"
        )
    )
    args = Array("--conf", "spark.api.mode=connect")
    assert(
      SparkPipelines.constructSparkSubmitArgs(
        pipelinesCliFile = "abc/python/pyspark/pipelines/cli.py", args = args) ==
        Seq(
          "--conf",
          "spark.api.mode=connect",
          "--remote",
          "local",
          "abc/python/pyspark/pipelines/cli.py"
        )
    )
    args = Array("--conf", "spark.api.mode= connect")
    assert(
      SparkPipelines.constructSparkSubmitArgs(
        pipelinesCliFile = "abc/python/pyspark/pipelines/cli.py", args = args) ==
        Seq(
          "--conf",
          "spark.api.mode=connect",
          "--remote",
          "local",
          "abc/python/pyspark/pipelines/cli.py"
        )
    )
    args = Array("-c", "spark.api.mode=connect")
    assert(
      SparkPipelines.constructSparkSubmitArgs(
        pipelinesCliFile = "abc/python/pyspark/pipelines/cli.py", args = args) ==
        Seq(
          "--conf",
          "spark.api.mode=connect",
          "--remote",
          "local",
          "abc/python/pyspark/pipelines/cli.py"
        )
    )
  }

  test("name arg") {
    val args = Array(
      "init",
      "--name",
      "myproject"
    )
    assert(
      SparkPipelines.constructSparkSubmitArgs(
        pipelinesCliFile = "abc/python/pyspark/pipelines/cli.py", args = args) ==
        Seq(
          "--conf",
          "spark.api.mode=connect",
          "--remote",
          "local",
          "abc/python/pyspark/pipelines/cli.py",
          "init",
          "--name",
          "myproject"
        )
    )
  }

  test("master arg is passed to spark-submit instead of the default remote") {
    Seq("local[*]", "local[2]", "yarn", "spark://host:7077", "k8s://https://host:443").foreach {
      master =>
        val args = Array("--master", master, "run", "--spec", "spark-pipeline.yml")
        assert(
          SparkPipelines.constructSparkSubmitArgs(
            pipelinesCliFile = "abc/python/pyspark/pipelines/cli.py", args = args) ==
            Seq(
              "--conf",
              "spark.api.mode=connect",
              "--master",
              master,
              "abc/python/pyspark/pipelines/cli.py",
              "run",
              "--spec",
              "spark-pipeline.yml"
            )
        )
    }
  }

  test("master and remote args are mutually exclusive") {
    val args = Array("--master", "local[*]", "--remote", "sc://host:15002", "run")
    intercept[SparkUserAppException] {
      SparkPipelines.constructSparkSubmitArgs(
        pipelinesCliFile = "abc/python/pyspark/pipelines/cli.py", args = args)
    }
  }

  test("client deploy mode is accepted and cluster deploy mode is rejected") {
    val args = Array("--master", "local[2]", "--deploy-mode", "client", "run")
    assert(
      SparkPipelines.constructSparkSubmitArgs(
        pipelinesCliFile = "abc/python/pyspark/pipelines/cli.py", args = args) ==
        Seq(
          "--conf",
          "spark.api.mode=connect",
          "--master",
          "local[2]",
          "abc/python/pyspark/pipelines/cli.py",
          "run"
        )
    )
    intercept[SparkUserAppException] {
      SparkPipelines.constructSparkSubmitArgs(
        pipelinesCliFile = "abc/python/pyspark/pipelines/cli.py",
        args = Array("--master", "yarn", "--deploy-mode", "cluster", "run"))
    }
  }
}
