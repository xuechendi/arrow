# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

context("General checks")

if (identical(Sys.getenv("TEST_R_WITH_ARROW"), "TRUE")) {
  testthat::test_that("Arrow C++ is available", {
    expect_true(arrow_available())
  })
}

r_only({
  test_that("assert_is", {
    x <- 42
    expect_true(assert_is(x, "numeric"))
    expect_true(assert_is(x, c("numeric", "character")))
    expect_error(assert_is(x, "factor"), 'x must be a "factor"')
    expect_error(
      assert_is(x, c("factor", "list")),
      'x must be a "factor" or "list"'
    )
    expect_error(
      assert_is(x, c("factor", "character", "list")),
      'x must be a "factor", "character", or "list"'
    )
  })
})
