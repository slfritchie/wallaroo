/*

Copyright 2017 The Wallaroo Authors.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 implied. See the License for the specific language governing
 permissions and limitations under the License.

*/

use "collections"
use "ponytest"

actor Main is TestList
  new make() =>
    None

  fun tag tests(test: PonyTest) =>
    test(_TestMakeHashPartitions)
    test(_TestGetWeights)

class iso _TestMakeHashPartitions is UnitTest
  """
  Basic test of making a simple HashPartition with 3 claimants (TODO vocab??).
  """
  fun name(): String =>
    "hash_partitions/line-" + __loc.line().string()

  fun ref apply(h: TestHelper) ? =>
    let n3: Array[String] val = recover ["n1"; "n2"; "n3"] end
    let hp = HashPartitions(n3)

    h.assert_eq[USize](n3.size(), hp.nodes.size())

    // There's no guarantee the following is true, but it probably is true.

    let got: Map[String,Bool] = got.create()
    for i in Range[USize](0,255) do
      let s = "key" + i.string()
      let m = hp.get_claimant_by_key(s)? // TODO make this fun not-partial
      got(m) = true
    end
    h.assert_eq[USize](got.size(), n3.size())
    for (c, res) in got.pairs() do
      h.assert_eq[Bool](n3.contains(c), true)
      h.assert_eq[Bool](res, true)
    end

class iso _TestGetWeights is UnitTest
  """
  Basic test of get_weights
  """
  fun name(): String =>
    "hash_partitions/line-" + __loc.line().string()

  fun ref apply(h: TestHelper) ? =>
    let n3: Array[String] val = recover ["n1"; "n2"; "n3"; "n4"] end
    let hp = HashPartitions(n3)

    let x = hp.get_weights()
    for (node, w) in x.pairs() do
      @printf[I32]("\tnode %s w %llx\n".cstring(), node.cstring(), w)
    end
    x("kljasdfkljdsajkladsf")?
