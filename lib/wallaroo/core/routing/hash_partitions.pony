use "collections"
use "crypto"
use "wallaroo_labs/mort"

class ref HashPartitions is (Equatable[HashPartitions] & Stringable)
  let lower_bounds: Array[U128] = lower_bounds.create()
  let sizes: Array[U128] = sizes.create()
  let nodes: Map[U128, String] = nodes.create()
  let _orig_weights: Array[(String,F64)] val

  new ref create(nodes': Array[String] val) =>
    let ns: Array[(String, U128)] iso = recover ns.create() end
    let size: U128 = U128.max_value() / nodes'.size().u128()

    for n in nodes'.values() do
      ns.push((n, size))
    end
    _orig_weights = recover [] end // TODO fix this despite testing-only use?
    create2(consume ns)

  new ref create_with_weights(nodes': Array[(String, F64)] val,
    decimal_digits: USize = 2)
  =>
    let nodes'': Array[(String, F64)] trn = recover nodes''.create() end
    var sum: F64 = 0.0
    let ns: Array[(String, U128)] iso = recover ns.create() end

    for (n, w) in nodes'.values() do
      let w' = RoundF64(w, decimal_digits)
      if w' > 0.0 then
        nodes''.push((n, w'))
      end
    end

    for (_, w) in nodes''.values() do
      sum = sum + w
    end
    for (n, w) in nodes''.values() do
      let fraction: F64 = w / sum
      let sz': F64 = U128.max_value().f64() * fraction
      let sz: U128 = U128.from[F64](sz')
      // @printf[I32]("node %s weight %d sum %.2f fraction %.1f\n".cstring(), n.cstring(), w, sum, fraction)
      ns.push((n, sz))
    end
    _orig_weights = consume nodes''
    create2(consume ns)

  fun ref create2(nodes': Array[(String, U128)] val) =>
    let count = nodes'.size()
    var next_lower_bound: U128 = 0

    try
      for i in Range[USize](0, count) do
        let node = nodes'(i)?._1
        let part_size = nodes'(i)?._2
        lower_bounds.push(next_lower_bound)
        sizes.push(part_size)
        nodes(next_lower_bound) = node
        next_lower_bound = next_lower_bound + part_size
      end
    else
      Fail()
    end

    var sum: U128 = 0
    for s in sizes.values() do
      sum = sum + s
    end
    let idx = lower_bounds.size() - 1
    let adjust = (U128.max_value() - sum)
    try sizes(idx)? = sizes(idx)? + adjust else Fail() end

  fun box eq(y: HashPartitions box): Bool =>
    try
      if (lower_bounds.size() == y.lower_bounds.size()) and
         (sizes.size() == y.sizes.size()) and
         (nodes.size() == y.nodes.size())
      then
        for i in lower_bounds.keys() do
          if (lower_bounds(i)? != y.lower_bounds(i)?) or
             (sizes(i)? != y.sizes(i)?) or
             (nodes(lower_bounds(i)?)? != y.nodes(y.lower_bounds(i)?)?)
          then
            return false
          end
        end
        return true
      else
        return false
      end
    else
      false
    end

  fun box ne(y: HashPartitions box): Bool =>
    not eq(y)

  fun box string(): String iso^ =>
    let s: String iso = "".clone()

    try
      for i in lower_bounds.keys() do
        s.append(nodes(lower_bounds(i)?)? + "@" + sizes(i)?.string() + ",")
      end
    else
      Fail()
    end
    consume s

  fun get_claimant(hash: U128): String ? =>
    var next_to_last_idx: USize = lower_bounds.size() - 1
    var last_idx: USize = 0

    if hash > lower_bounds(next_to_last_idx)? then
      return nodes(lower_bounds(next_to_last_idx)?)?
    end

    // Binary search
    while true do
      let next_lower_bound = lower_bounds(last_idx)?

      if hash >= next_lower_bound then
        if hash < lower_bounds(last_idx + 1)? then
          return nodes(next_lower_bound)?
        else
          var step =
            (next_to_last_idx.isize() - last_idx.isize()).abs().usize() / 2
          if step == 0 then step = 1 end
          next_to_last_idx = last_idx
          last_idx = last_idx + step
        end
      else
        var step =
          (next_to_last_idx.isize() - last_idx.isize()).abs().usize() / 2
        if step == 0 then step = 1 end
        next_to_last_idx = last_idx
        last_idx = last_idx - step
      end
    end
    Unreachable()
    ""

  fun get_claimant_by_key(key: ByteSeq): String ? =>
    var hashed_key: U128 = 0
    for v in MD5(key).values() do
      hashed_key = (hashed_key << 8) + v.u128()
    end
    get_claimant(hashed_key)?

  fun claimants(): Iterator[String] ref =>
    nodes.values()

  fun get_weights(): Map[String,U128] =>
    let w: Map[String,U128] = w.create()

    try
      for node in nodes.values() do
        w(node) = 0
      end

      for i in Range[USize](0, lower_bounds.size()) do
        let node = nodes(lower_bounds(i)?)?
        w(node) = w(node)? + sizes(i)?
      end
    else
      Fail()
    end
    w

  fun get_weights_unit_interval(): Map[String,F64] =>
    let w: Map[String, F64] = w.create()

    for (node, weight) in get_weights().pairs() do
      w(node) = weight.f64() / U128.max_value().f64()
    end
    w

  fun get_weights_normalized(decimal_digits: USize = 2): Map[String,F64] =>
    let ws: Map[String, F64] = ws.create()
    var min_weight = F64.max_value()
    let weights = get_weights_unit_interval()

    for (_, weight) in weights.pairs() do
      min_weight = min_weight.min(weight)
    end
    for (node, weight) in weights.pairs() do
      ws(node) = RoundF64(weight.f64() / min_weight, decimal_digits)
    end
    ws

  fun pretty_print() =>
    for (n, w) in normalize().values() do
      @printf[I32]("node %10s relative-size %.4f\n".cstring(), n.cstring(), w)
    end

  fun normalize(decimal_digits: USize = 2): Array[(String, F64)] =>
    var min_size: U128 = U128.max_value()
    let ns: Array[(String, F64)] = ns.create()

    try
      for i in Range[USize](0, sizes.size()) do
        min_size = min_size.min(sizes(i)?)
      end

      for i in Range[USize](0, sizes.size()) do
        let w = RoundF64(sizes(i)?.f64() / min_size.f64(), decimal_digits)
        ns.push((nodes(lower_bounds(i)?)?, w))
      end
    else
      Fail()
    end
    ns

  fun adjust_weights(nodes': Array[(String, F64)] val,
    decimal_digits: USize = 2): HashPartitions
   =>
    //// Figure out what nodes have been removed from the current list

    let current_weights = get_weights_normalized(decimal_digits)
    var current_nodes: SetIs[String] = current_nodes.create()
    var new_nodes: SetIs[String] = new_nodes.create()

    for (n, w') in _orig_weights.values() do
      current_nodes = current_nodes.add(n)
    end
    for (n, w') in nodes'.values() do
      new_nodes = new_nodes.add(n)
    end

    let removed_nodes = current_nodes.without(new_nodes)
    let added_nodes = new_nodes.without(current_nodes)
    @printf[I32]("Removed nodes: ".cstring())
    for n in removed_nodes.values() do
      @printf[I32]("%s, ".cstring(), n.cstring())
    end
    @printf[I32]("\n".cstring())
    @printf[I32]("Added nodes: ".cstring())
    for n in added_nodes.values() do
      @printf[I32]("%s, ".cstring(), n.cstring())
    end
    @printf[I32]("\n".cstring())

    // TEST HACK: Create test failure: use _orig_weights to force test failure
    HashPartitions.create_with_weights(_orig_weights)

  // Hmm, do I want this mutating thingie in here at all?
  fun ref twiddle(from: String, to: String) =>
    for (lb, s) in nodes.pairs() do
      if s == from then
        nodes(lb) = to
        return
      end
    end

primitive RoundF64
  fun apply(f: F64, decimal_digits: USize = 2): F64 =>
    let factor = F64(10).pow(decimal_digits.f64())

    ((f * factor) + 0.5).trunc() / factor
