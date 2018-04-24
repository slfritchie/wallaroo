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

  new create(env: Env) =>
    PonyTest(env, this)

  fun tag tests(test: PonyTest) =>
    test(_TestMakeHashPartitions)
    test(_TestMakeHashPartitions2)

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
    hp.pretty_print()

class iso _TestMakeHashPartitions2 is UnitTest
  """
  Basic test of get_weights
  """
  fun name(): String =>
    "hash_partitions/line-" + __loc.line().string()

  fun ref apply(h: TestHelper) /**?**/ =>
    @printf[I32]("\n".cstring())
    let n1: Array[(String,U128)] val = recover
      [("n1", 1*1); ("n2", 2*1); ("n3", 3*1); ("n4", 1*1)] end
    let n2: Array[(String,U128)] val = recover
      [("n1", 1*3); ("n2", 2*3); ("n3", 3*3); ("n4", 1*3)] end
    let n3: Array[(String,U128)] val = recover
     [("n1", 48611766702991206367701239421883908096)
      ("n2", 97223533405982412735402478843767816192)
      ("n3", 145835300108973619103103718265651724288)
      ("n4", 48611766702991225257167170900464762879)] end
    let n4: Array[(String,U128)] val = recover
     [("n1", 48611766702991206367701239421883908096/15)
      ("n2", 97223533405982412735402478843767816192/15)
      ("n3", 145835300108973619103103718265651724288/15)
      ("n4", 48611766702991225257167170900464762879/15)] end
    let hp1 = HashPartitions.create_with_weights(n1)
    let hp2 = HashPartitions.create_with_weights(n2)
    let hp3 = HashPartitions.create_with_weights(n3)
    let hp4 = HashPartitions.create_with_weights(n4)

    for hp in [hp1; hp2; hp3; hp4].values() do
      @printf[I32]("\n".cstring())
      for (node, w) in hp.get_weights().pairs() do
        @printf[I32]("\tnode %s w %s\n".cstring(), node.cstring(), w.string().cstring())
      end
      hp.pretty_print()
      @printf[I32]("\n".cstring())
    end
