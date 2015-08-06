#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

<<<<<<< HEAD:R/pkg/R/zzz.R
.onLoad <- function(libname, pkgname) {
  sparkR.onLoad(libname, pkgname)
}
=======
import os

SPARK_HOME = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../"))
USER_HOME = os.environ.get("HOME")
>>>>>>> 4399b7b0903d830313ab7e69731c11d587ae567c:dev/sparktestsupport/__init__.py
