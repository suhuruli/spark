@echo off

rem
rem Licensed to the Apache Software Foundation (ASF) under one or more
rem contributor license agreements.  See the NOTICE file distributed with
rem this work for additional information regarding copyright ownership.
rem The ASF licenses this file to You under the Apache License, Version 2.0
rem (the "License"); you may not use this file except in compliance with
rem the License.  You may obtain a copy of the License at
rem
rem    http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.
rem

rem Figure out where the Spark framework is installed
call "%~dp0find-spark-home.cmd"

call "%SPARK_HOME%\bin\load-spark-env.cmd"

rem Default to standard python3 interpreter unless told otherwise
if "x%PYSPARK_PYTHON%"=="x" (
  set PYSPARK_PYTHON=python3
)

rem Add the PySpark classes to the Python path:
set PYTHONPATH=%SPARK_HOME%\python;%PYTHONPATH%
set PYTHONPATH=%SPARK_HOME%\python\lib\py4j-0.10.9.9-src.zip;%PYTHONPATH%

rem Locate the Python CLI implementation relative to the pyspark package that PYSPARK_PYTHON
rem imports, the same way bin/spark-pipelines does, so that both a Spark distribution and a
rem pip-installed pyspark resolve the right file.
set SDP_CLI_PY_FILE_PATH=
for /f "usebackq delims=" %%i in (`%PYSPARK_PYTHON% -c "import pyspark, os; from pathlib import Path; print(Path(os.path.dirname(pyspark.__file__)) / 'pipelines' / 'cli.py')"`) do set SDP_CLI_PY_FILE_PATH=%%i

if "x%SDP_CLI_PY_FILE_PATH%"=="x" (
  echo Failed to locate pyspark/pipelines/cli.py with the Python executable '%PYSPARK_PYTHON%'.
  echo Install Python with PySpark, or set PYSPARK_PYTHON to a Python executable that can import pyspark.
  exit /b 1
)

call "%SPARK_HOME%\bin\spark-class2.cmd" org.apache.spark.deploy.SparkPipelines "%SDP_CLI_PY_FILE_PATH%" %*
