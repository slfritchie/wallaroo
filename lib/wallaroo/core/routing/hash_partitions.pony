use "collections"
use "crypto"
use "wallaroo_labs/mort"

class ref HashPartitions is (Equatable[HashPartitions] & Stringable)
  let lower_bounds: Array[U128] = lower_bounds.create()
  let interval_sizes: Array[U128] = interval_sizes.create()
  let lb_to_c: Map[U128, String] = lb_to_c.create()  // lower bound -> claimant
  let _orig_weights: Array[(String,F64)] val

  new ref create(cs: Array[String] val) =>
    """
    Given an array of claimant name strings that are non-zero length,
    create a HashPartitions that assigns equal weight to all claimants.
    """
    let sizes: Array[(String, U128)] iso = recover sizes.create() end
    let interval: U128 = U128.max_value() / cs.size().u128()

    for c in cs.values() do
      if c != "" then
        sizes.push((c, interval))
      end
    end
    _orig_weights = recover [] end // TODO fix this despite testing-only use?
    create2(consume sizes)

  new ref create_with_weights(weights: Array[(String, F64)] val,
    decimal_digits: USize = 2)
  =>
    """
    Given an array of 2-tuples (claimant name strings that are
    non-zero length plus floating point weighting factor for the claimant),
    create a HashPartitions that assigns equal weight to all claimants.
    """
    let weights': Array[(String, F64)] trn = recover weights'.create() end
    var sum: F64 = 0.0
    let sizes: Array[(String, U128)] iso = recover sizes.create() end

    for (c, w) in weights.values() do
      let w' = RoundF64(w, decimal_digits)
      if (c != "") and (w' > 0.0) then
        weights'.push((c, w'))
      end
    end

    for (_, w) in weights'.values() do
      sum = sum + w
    end
    for (c, w) in weights'.values() do
      let fraction: F64 = w / sum
      let sz': F64 = U128.max_value().f64() * fraction
      let sz: U128 = U128.from[F64](sz')
      // @printf[I32]("node %s weight %d sum %.2f fraction %.1f\n".cstring(), c.cstring(), w, sum, fraction)
      sizes.push((c, sz))
    end
    _orig_weights = consume weights'
    create2(consume sizes)

  fun ref create2(sizes: Array[(String, U128)] val) =>
    """
    Create a HashPartitions using U128-sized intervals.
    """
    let count = sizes.size()
    var next_lower_bound: U128 = 0

    try
      for i in Range[USize](0, count) do
        let c = sizes(i)?._1
        let interval = sizes(i)?._2
        lower_bounds.push(next_lower_bound)
        interval_sizes.push(interval)
        lb_to_c(next_lower_bound) = c
        next_lower_bound = next_lower_bound + interval
      end
    else
      Fail()
    end

    var sum: U128 = 0
    for interval in interval_sizes.values() do
      sum = sum + interval
    end
    let idx = lower_bounds.size() - 1
    let i_adjust = (U128.max_value() - sum)
                            @printf[I32]("\ti_adjust = %ld\n".cstring(), i_adjust.u64())
    try interval_sizes(idx)? = interval_sizes(idx)? + i_adjust else Fail() end

  fun box eq(y: HashPartitions box): Bool =>
    """
    Implement eq for Equatable trait.
    """
    try
      if (lower_bounds.size() == y.lower_bounds.size()) and
         (interval_sizes.size() == y.interval_sizes.size()) and
         (lb_to_c.size() == y.lb_to_c.size())
      then
        for i in lower_bounds.keys() do
          if (lower_bounds(i)? != y.lower_bounds(i)?) or
             (interval_sizes(i)? != y.interval_sizes(i)?) or
             (lb_to_c(lower_bounds(i)?)? != y.lb_to_c(y.lower_bounds(i)?)?)
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
    """
    Implement eq for Equatable trait.
    """
    not eq(y)

  fun box string(): String iso^ =>
    """
    Implement string for Stringable trait.
    """
    let s: String iso = "".clone()

    try
      for i in lower_bounds.keys() do
        s.append(lb_to_c(lower_bounds(i)?)? + "@" +
          interval_sizes(i)?.string() + ",")
      end
    else
      Fail()
    end
    consume s

  fun get_claimant(hash: U128): String ? =>
    var next_to_last_idx: USize = lower_bounds.size() - 1
    var last_idx: USize = 0

    if hash > lower_bounds(next_to_last_idx)? then
      return lb_to_c(lower_bounds(next_to_last_idx)?)?
    end

    // Binary search
    while true do
      let next_lower_bound = lower_bounds(last_idx)?

      if hash >= next_lower_bound then
        if hash < lower_bounds(last_idx + 1)? then
          return lb_to_c(next_lower_bound)?
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
    lb_to_c.values()

  fun _get_interval_size_sums(): Map[String,U128] =>
    let interval_sums: Map[String,U128] = interval_sums.create()

    try
      for c in lb_to_c.values() do
        interval_sums(c) = 0
      end

      for i in Range[USize](0, lower_bounds.size()) do
        let c = lb_to_c(lower_bounds(i)?)?
        interval_sums(c) = interval_sums(c)? + interval_sizes(i)?
      end
    else
      Fail()
    end
    interval_sums

  fun get_weights_unit_interval(): Map[String,F64] =>
    let ws: Map[String, F64] = ws.create()

    for (c, sum) in _get_interval_size_sums().pairs() do
      ws(c) = sum.f64() / U128.max_value().f64()
    end
    ws

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
    for (n, w) in _normalize_for_pp().values() do
      @printf[I32]("node %10s relative-size %.4f\n".cstring(), n.cstring(), w)
    end

  fun _normalize_for_pp(decimal_digits: USize = 2): Array[(String, F64)] =>
    var min_size: U128 = U128.max_value()
    let ns: Array[(String, F64)] = ns.create()

    try
      for i in Range[USize](0, interval_sizes.size()) do
        min_size = min_size.min(interval_sizes(i)?)
      end

      for i in Range[USize](0, interval_sizes.size()) do
        let w = RoundF64(interval_sizes(i)?.f64() / min_size.f64(), decimal_digits)
        ns.push((lb_to_c(lower_bounds(i)?)?, w))
      end
    else
      Fail()
    end
    ns

  fun adjust_weights(new_weights: Array[(String, F64)] val,
    decimal_digits: USize = 2): HashPartitions
   =>
    let new_weights' = new_weights.clone()
    //// Figure out what claimants have been removed.

    let current_weights = get_weights_normalized(decimal_digits)
    var current_cs: SetIs[String] = current_cs.create()
    var new_cs: SetIs[String] = new_cs.create()

    for (c, w') in _orig_weights.values() do
      current_cs = current_cs.add(c)
    end
    for (c, w') in new_weights.values() do
      new_cs = new_cs.add(c)
    end
    let removed_cs = current_cs.without(new_cs)
                        @printf[I32]("Removed claimants: ".cstring())
                        for c in removed_cs.values() do
                          @printf[I32]("%s, ".cstring(), c.cstring())
                        end
                        @printf[I32]("\n".cstring())

    //// Assign weights of zero to claimants not in the new list
    for c in removed_cs.values() do
                          @printf[I32]("Add claimant %s with weight 0 to weights'\n".cstring(), c.cstring())
      new_weights'.push((c, 0.0))
    end

    let added_cs = new_cs.without(current_cs)
                        @printf[I32]("Added claimants: ".cstring())
                        for c in added_cs.values() do
                          @printf[I32]("%s, ".cstring(), c.cstring())
                        end
                        @printf[I32]("\n".cstring())



    // TEST HACK: Create test failure: use _orig_weights to force test failure
    HashPartitions.create_with_weights(_orig_weights)

  // Hmm, do I want this mutating thingie in here at all?
  fun ref twiddle(from: String, to: String) =>
    for (lb, c) in lb_to_c.pairs() do
      if c == from then
        lb_to_c(lb) = to
        return
      end
    end

primitive RoundF64
  fun apply(f: F64, decimal_digits: USize = 2): F64 =>
    let factor = F64(10).pow(decimal_digits.f64())

    ((f * factor) + 0.5).trunc() / factor
