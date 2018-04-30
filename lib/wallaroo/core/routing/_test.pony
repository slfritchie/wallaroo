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
use "ponycheck"
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
    test(Property1UnitTest[(Array[TestOp])](_TestPonycheckStateful))


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
    let hp2c = try hp1.remove_claimants(recover ["n3"] end)?
      else /* super-bogus value */ HashPartitions.create([]) end

    // 4 -> 3: weights of direct path = weights of adjusted HashPartitions
    let norm_w_hp2a = hp2a.get_weights_normalized()
    let norm_w_hp2b = hp2b.get_weights_normalized()
    let norm_w_hp2c = hp2c.get_weights_normalized()
    CompareWeights(norm_w_hp2a, norm_w_hp2b, "", __loc.line())?
    CompareWeights(norm_w_hp2a, norm_w_hp2c, "", __loc.line())?

    // n -> n+1: weights of direct path = weights of adjusted HashPartitions
    let weights = weights2.clone()
    var more: Array[String] val = recover ["n5"; "n6"; "n7"; "n8"] end
    var hpb = hp2b
    for c in more.values() do
      weights.push((c, 1))
      let ws: Array[(String, F64)] val = copyit(weights)
      let hpa = HashPartitions.create_with_weights(ws)
      hpb = hpb.adjust_weights(ws)

      let norm_w_hpa = hpa.get_weights_normalized()
      let norm_w_hpb = hpb.get_weights_normalized()
      CompareWeights(norm_w_hpa, norm_w_hpb, c, __loc.line())?
    end

    // n -> n+8: weights of direct path = weights of adjusted HashPartitions
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

    // n -> n+1: weights of direct path = weights of adjusted HashPartitions
    more = recover 
      ["n17"; "n18"; "n19"; "n20"; "n21"; "n22"; "n24"; "n25"; "n26"; "n27"]end
    for c in more.values() do
      weights.push((c, 1))
      let ws: Array[(String, F64)] val = copyit(weights)
      let hpa = HashPartitions.create_with_weights(ws)
      hpb = hpb.adjust_weights(ws)

      let norm_w_hpa = hpa.get_weights_normalized()
      let norm_w_hpb = hpb.get_weights_normalized()
      CompareWeights(norm_w_hpa, norm_w_hpb, c, __loc.line())?
    end

for (cc, ii) in hpb.get_weights_unit_interval().pairs() do @printf[I32]("**weights unit interval**: c %s weight%% %.10f\n".cstring(), cc.cstring(), ii*100.0) end ; @printf[I32]("\n".cstring())
for (cc, ii) in hpb.get_weights_normalized().pairs() do @printf[I32]("**weights unit interval**: c %s norm-weight %.10f\n".cstring(), cc.cstring(), ii) end ; @printf[I32]("\n".cstring())
    // n -> n-2: weights of direct path = weights of adjusted HashPartitions
    while weights.size() > 3 do
@printf[I32]("********** size=%d ****************************\n\n".cstring(), weights.size())
      try
        (let c_r1, _) = weights(weights.size()-2)?
        (let c_r2, _) = weights(1)?
        weights.remove(weights.size()-2, 1)
        weights.remove(1, 1)
        let ws: Array[(String, F64)] val = copyit(weights)
        let hpa = HashPartitions.create_with_weights(ws)
        // Test both adjust_weights() and remove_claimants() API
        hpb = if weights.size() > 16 then
            hpb.adjust_weights(ws)
          else
            hpb.remove_claimants(recover [c_r1; c_r2] end)?
          end

        let norm_w_hpa = hpa.get_weights_normalized()
        let norm_w_hpb = hpb.get_weights_normalized()
for (cc, ii) in hpb.get_weights_unit_interval().pairs() do @printf[I32]("**weights unit interval**: c %s weight%% %.10f\n".cstring(), cc.cstring(), ii*100.0) end ; @printf[I32]("\n".cstring())
        CompareWeights(norm_w_hpa, norm_w_hpb, "foo", __loc.line())?
      end
    else
      Fail()
    end
for (cc, ii) in hpb.get_weights_unit_interval().pairs() do @printf[I32]("**weights unit interval**: c %s weight%% %.10f\n".cstring(), cc.cstring(), ii*100.0) end ; @printf[I32]("\n".cstring())
for (cc, ii) in hpb.get_weights_normalized().pairs() do @printf[I32]("**weights unit interval**: c %s norm-weight %.10f\n".cstring(), cc.cstring(), ii) end ; @printf[I32]("\n".cstring())


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
      @printf[I32]("Map a\n".cstring())
      // LOL nope: let a_ks = a.keys().collect(Array[String]).sort()
      // LOL nope: for k in Sort[Array[String],String](a_ks).values() do
      for k in a.keys() do
        try @printf[I32]("-> c %s weight %.2f\n".cstring(), k.cstring(), a(k)?) end
      end
      @printf[I32]("Map b\n".cstring())
      for k in b.keys() do
        try @printf[I32]("-> c %s weight %.2f\n".cstring(), k.cstring(), b(k)?) end
      end

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

/*
** Let's try to make a stateful Ponycheck test case.  Unfortunately,
** Ponycheck doesn't support stateful properties.  So we need to fake it.
**
** 1. Our generator will create a sequence of TestOp objects, one TestOp
**    for each state change we wish to make to the system.  ("system" is
**    the HashPartitions class.)
**    With Erlang QuickCheck or PropEr, each op would be a 2-tuple like
**    {'add', Names::List(String)} or {'remove', Names::List(String)}
**
** 2. We "apply" the TestOp's method to the SUT (system under test) and to
**    our model.
**    With Erlang QuickCheck or PropEr, there's framework support for
**    doing this in a 1-step-at-a-time manner.  Ponycheck has no such
**    framework yet, so we have to implement it ourselves.
**
** 3. After each apply, check that the SUT and our model are equivalent.
**    We also check for other desirable properties, e.g., the sum of all
**    intervals is equal to 2^128.
*/

primitive HashOpAdd is Stringable
  fun string(): String iso^ => "add_claimants".clone()

primitive HashOpRemove is Stringable
  fun string(): String iso^ => "remove_claimants".clone()

type HashOp is (HashOpAdd | HashOpRemove)

class TestOp is Stringable
  let op: HashOp
  let cs: Array[String] = cs.create()

  new create(op': HashOp, n_seq: Array[USize]) =>
    op = op'
    for n in n_seq.values() do
      cs.push("n" + n.string())
    end
    // @printf[I32]("CREATE: %s\n".cstring(), this.string().cstring())

  fun string(): String iso^ =>
    let s: String ref = recover s.create() end

    s.append("TestOp: op=" + op.string() + ",cs=[")
    for c in cs.values() do
      s.append(c + ",")
    end
    s.append("]")
    s.clone()


class _TestPonycheckStateful is Property1[(Array[TestOp])]
  fun name(): String => "hash_partitions/ponycheck"

  fun gen(): Generator[Array[TestOp]] =>
    // Bah, one_of() is partial, which is a PITA, so let's frequency()
    let gen_hash_op = Generators.frequency[HashOp]([
      (1, HashOpAdd); (2, HashOpRemove)])

    // We are going to generate Array[USize] and rely on TestOp.create()
    // to convert the integers into claimant name strings.
    let gen_ns = Generators.seq_of[USize, Array[USize]](
      Generators.usize(where min=0, max=12))

    let gen_testop = Generators.map2[HashOp, Array[USize], TestOp](
      gen_hash_op, gen_ns,
      {(hash_op, n_seq) => TestOp(hash_op, n_seq)}
    )
    gen_testop


  fun property(arg1: Array[TestOp], ph: PropertyHelper) =>
    @printf[I32]("PROP:\n".cstring())
    for to in arg1.values() do
      @printf[I32]("    %s\n".cstring(), to.string())
    end
    @printf[I32]("\n".cstring())

    true
