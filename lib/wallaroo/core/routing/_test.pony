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
use "wallaroo_labs/mort"

actor Main is TestList
  new make() =>
    None

  new create(env: Env) =>
    PonyTest(env, this)

  fun tag tests(test: PonyTest) =>
    test(_TestMakeHashPartitions)
    test(_TestMakeHashPartitions2)
    test(_TestMakeHashPartitions3)
    test(_TestAdjustHashPartitions)

class iso _TestMakeHashPartitions is UnitTest
  """
  Basic test of making a simple HashPartition with 3 claimants (TODO vocab??).
  """
  fun name(): String =>
    "hash_partitions/line-" + __loc.line().string()

  fun ref apply(h: TestHelper) ? =>
    let n3: Array[String] val = recover ["n1"; "n2"; "n3"] end
    let hp = HashPartitions(n3)
    // hp.pretty_print()

    h.assert_eq[USize](n3.size(), hp.lb_to_c.size())

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

class iso _TestMakeHashPartitions2 is UnitTest
  """
  Make several HashPartitions that should be equivalent
  """
  fun name(): String =>
    "hash_partitions/line-" + __loc.line().string()

  fun ref apply(h: TestHelper) /**?**/ =>
    @printf[I32]("\n".cstring())
    let weights1: Array[(String,F64)] val = recover
      [("n1", 1*1); ("n2", 2*1); ("n3", 3*1); ("n4", 1*1)] end
    let weights2: Array[(String,F64)] val = recover
      [("n1", 1*3); ("n2", 2*3); ("n3", 3*3); ("n4", 1*3)] end
    let weights3: Array[(String,F64)] val = recover
     [("n1", 48611766702991206367701239421883908096)
      ("n2", 97223533405982412735402478843767816192)
      ("n3", 145835300108973619103103718265651724288)
      ("n4", 48611766702991225257167170900464762879)] end
    let weights4: Array[(String,F64)] val = recover
     [("n1", 48611766702991206367701239421883908096/15)
      ("n2", 97223533405982412735402478843767816192/15)
      ("n3", 145835300108973619103103718265651724288/15)
      ("n4", 48611766702991225257167170900464762879/15)] end
    let hp1 = HashPartitions.create_with_weights(weights1)
    let hp2 = HashPartitions.create_with_weights(weights2)
    let hp3 = HashPartitions.create_with_weights(weights3)
    let hp4 = HashPartitions.create_with_weights(weights4)

    // hp1.pretty_print()
    // All 4 HashPartitions should be exactly equal
    h.assert_eq[HashPartitions](hp1, hp2)
    h.assert_eq[HashPartitions](hp1, hp3)
    h.assert_eq[HashPartitions](hp1, hp4)

class iso _TestMakeHashPartitions3 is UnitTest
  """
  Basic test of making HashPartitions with weights
  """
  fun name(): String =>
    "hash_partitions/line-" + __loc.line().string()

  fun ref apply(h: TestHelper) /**?**/ =>
    // This is an intentionally choppy way of making a map for 4 nodes
    // Total ratio should be 1 : 12 : 3 : 1.
    let weights1: Array[(String,F64)] val = recover
      [("n1", 1*1); ("n2", 2*7); ("n3", 3*1); ("n4", 1*1)
       ("n1", 1*1); ("n2", 2*6); ("n3", 3*1); ("n4", 1*1)
       ("n1", 1*1); ("n2", 2*5); ("n3", 3*1); ("n4", 1*1)] end

    let hp1 = HashPartitions.create_with_weights(weights1)
    // hp1.pretty_print() ; @printf[I32]("\n".cstring())

    for (n, w) in hp1.get_weights_normalized().pairs() do
      match n
      | "n1" => h.assert_eq[F64](w, 1.0)
      | "n2" => h.assert_eq[F64](w, 12.0)
      | "n3" => h.assert_eq[F64](w, 3.0)
      | "n4" => h.assert_eq[F64](w, 1.0)
      end
    end

class iso _TestAdjustHashPartitions is UnitTest
  """
  Basic test of adjusting a HashPartitions with added & removed nodes
  """
  fun name(): String =>
    "hash_partitions/line-" + __loc.line().string()

  fun ref apply(h: TestHelper) ? =>
    let weights1: Array[(String,F64)] val = recover
      [("n1", 1); ("n2", 2); ("n3", 3); ("n4", 4)] end
    let hp1 = HashPartitions.create_with_weights(weights1)

    let weights2: Array[(String,F64)] val = recover
      [("n1", 1); ("n2", 2);            ("n4", 4)] end
    let hp2a = HashPartitions.create_with_weights(weights2)
    let hp2b = hp1.adjust_weights(weights2)

    // TODO: What we ought to test for here:
    // The normalized weights of hp2a are the same as
    // the normalized weights of hp2b!

    let norm_w_hp2a = hp2a.get_weights_normalized()
    let norm_w_hp2b = hp2b.get_weights_normalized()
    CompareWeights(norm_w_hp2a, norm_w_hp2b, "", __loc.line())?

@printf[I32]("**************************************\n\n".cstring())
    let weights = weights2.clone()
    let more: Array[String] val = recover ["n5"; "n6"; "n7"; "n8"] end
    var hpb = hp2b
    for c in more.values() do
      weights.push((c, 1))
      let ws: Array[(String, F64)] val = copyit(weights)
      let hpa = HashPartitions.create_with_weights(ws)
@printf[I32]("***** %s *********************************\n".cstring(), c.cstring())
for (cc, ww) in ws.values() do @printf[I32]("\tws: c %s weight %.2f\n".cstring(), cc.cstring(), ww) end ; @printf[I32]("\n".cstring())
for (cc, ss) in hpb.get_sizes().values() do @printf[I32]("*before adjust*: c %s size %.10f\n".cstring(), cc.cstring(), (ss.f64()/U128.max_value().f64())*100.0) end ; @printf[I32]("\n".cstring())
      hpb = hpb.adjust_weights(ws)
for (cc, ss) in hpb.get_sizes().values() do @printf[I32]("*after adjust*: c %s size %.10f\n".cstring(), cc.cstring(), (ss.f64()/U128.max_value().f64())*100.0) end ; @printf[I32]("\n".cstring())
for (cc, ii) in hpb.get_weights_unit_interval().pairs() do @printf[I32]("*weights unit interval*: c %s size %.10f\n".cstring(), cc.cstring(), ii*100.0) end ; @printf[I32]("\n".cstring())

      let norm_w_hpa = hpa.get_weights_normalized()
      let norm_w_hpb = hpb.get_weights_normalized()
      CompareWeights(norm_w_hpa, norm_w_hpb, c, __loc.line())?
    end

    for c in ["n9"; "n10"; "n11"; "n12"; "n13"; "n14"; "n15"; "n16"].values() do
      weights.push((c,1))
    end
    let ws16: Array[(String, F64)] val = copyit(weights)
    let hpa16 = HashPartitions.create_with_weights(ws16)
    hpb = hpb.adjust_weights(ws16)
    let norm_w_hpa16 = hpa16.get_weights_normalized()
    let norm_w_hpb16 = hpb.get_weights_normalized()
    CompareWeights(norm_w_hpa16, norm_w_hpb16, "up to n16", __loc.line())?
for (cc, ii) in hpb.get_weights_unit_interval().pairs() do @printf[I32]("*weights unit interval*: c %s size %.10f\n".cstring(), cc.cstring(), ii*100.0) end ; @printf[I32]("\n".cstring())


    // TODO: then we ought to have a boatload of PonyCheck tests!  ^_^

    fun copyit(src: Array[(String, F64)]): Array[(String, F64)] iso^ =>
      let dst: Array[(String, F64)] iso = recover dst.create() end

      for x in src.values() do
        dst.push(x)
      end
      consume dst

primitive CompareWeights
  fun apply(a: Map[String, F64], b: Map[String, F64],
    extra: String, test_line: USize) ?
   =>
    if a.size() != b.size() then
      @printf[I32]("Map size error: %d != %d at test_line %d extra %s\n".cstring(),
       a.size(), b.size(), test_line, extra.cstring())
      error
    end
    for (c, s) in a.pairs() do
      try
        if s != b(c)? then
          @printf[I32]("Map error: claimant %s, a interval size %.20f b interval size %.20f test_line %d extra %s\n".cstring(), c.cstring(), s, b(c)?, test_line, extra.cstring())
          error
        end
      else
          @printf[I32]("Map error: claimant %s does not exist in map b test_line %d extra %s\n".cstring(), c.cstring(), test_line, extra.cstring())
          error
      end
    end
