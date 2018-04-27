use "collections"
use "crypto"
use "wallaroo_labs/mort"

class ref HashPartitions is (Equatable[HashPartitions] & Stringable)
  let lower_bounds: Array[U128] = lower_bounds.create()
  let interval_sizes: Array[U128] = interval_sizes.create()
  let lb_to_c: Map[U128, String] = lb_to_c.create()  // lower bound -> claimant

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
    let sizes: Array[(String, U128)] trn = recover sizes.create() end

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
    create2(consume sizes)

  new ref create_with_sizes(sizes: Array[(String, U128)] val) =>
    create2(sizes)

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

  fun get_sizes(): Array[(String, U128)] =>
    let s: Array[(String, U128)] = s.create()

    try 
      for i in Range[USize](0, lower_bounds.size()) do
        let c = lb_to_c(lower_bounds(i)?)?
        s.push((c, interval_sizes(i)?))
      end
    else
      Fail()
    end
    s

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
    //// Figure out what claimants have been removed.

    let current_weights = get_weights_normalized(decimal_digits)
    var current_cs: SetIs[String] = current_cs.create()
    let current_sizes_m = _get_interval_size_sums()
    var new_cs: SetIs[String] = new_cs.create()
    let new_weights_m: Map[String, F64] = new_weights_m.create()
    let new_sizes_m: Map[String, U128] = new_sizes_m.create()
    var sum_new_weights: F64 = 0.0

    for (c, w) in get_weights_normalized().pairs() do
      current_cs = current_cs.add(c)
    @printf[I32]("Hmmm, newcurrent_cs.size = %d after c = %s\n".cstring(), current_cs.size(), new_cs.size(), c.cstring())
    end
    for (c, w) in new_weights.values() do
      new_cs = new_cs.add(c)
      let w' = RoundF64(w, decimal_digits)
      sum_new_weights = sum_new_weights + w'
      new_weights_m(c) = w'
      if not current_sizes_m.contains(c) then
        current_sizes_m(c) = 0
      end
    end
    @printf[I32]("Hmmm, current_cs.size = %d new_cs.size = %d\n".cstring(), current_cs.size(), new_cs.size())
    let removed_cs = current_cs.without(new_cs)
                        @printf[I32]("Removed claimants: ".cstring())
                        for c in removed_cs.values() do
                          @printf[I32]("%s, ".cstring(), c.cstring())
                        end
                        @printf[I32]("\n".cstring())

    //// Assign weights of zero to claimants not in the new list
    for c in removed_cs.values() do
                          @printf[I32]("Add claimant %s with weight 0 to new_weights_m\n".cstring(), c.cstring())
      new_weights_m(c) = 0.0
    end
                        @printf[I32]("new_weights_m.size() = %d\n".cstring(), new_weights_m.size())

    //// Calculate the interval slices that need to be redistributed
    let size_add: Map[String,U128] = size_add.create()
    let size_sub: Map[String,U128] = size_sub.create()

    try
      for (c, w) in new_weights_m.pairs() do
        let new_size = U128.from[F64]((w / sum_new_weights) * U128.max_value().f64())
        if new_size > current_sizes_m(c)? then
          size_add(c) = (new_size - current_sizes_m(c)?)
                          @printf[I32]("size_add: c %s add %5.2f%%\n".cstring(), c.cstring(), (size_add(c)?.f64()/U128.max_value().f64())*100.0)
        elseif new_size < current_sizes_m(c)? then
          size_sub(c) = (current_sizes_m(c)? - new_size)
                          @printf[I32]("size_sub: c %s sub %5.2f%%\n".cstring(), c.cstring(), (size_sub(c)?.f64()/U128.max_value().f64())*100.0)
        else
                          @printf[I32]("interval_***: c %s\n".cstring(), c.cstring())
          None
        end
      end
    else
      Fail()
    end

    //// Get the current map in Array[(Claimant,Size)] format, which
    //// is what create2() requires.

    let sizes1 = get_sizes()
                          for (c, s) in sizes1.values() do @printf[I32]("    sizes1 claimant %s size %5.2f%%\n".cstring(), c.cstring(), (s.f64()/U128.max_value().f64())*100.0) end ; @printf[I32]("\n".cstring())
    //// Process subtractions first: use the claimant name ""
    //// for unclaimed intervals.
    let sizes2 = _process_subtractions(sizes1, size_sub)
                          for (c, s) in sizes2.values() do @printf[I32]("    sizes2 claimant %s size %5.2f%%\n".cstring(), c.cstring(), (s.f64()/U128.max_value().f64())*100.0) end ; @printf[I32]("\n".cstring())
try
    let wws: Map[String, F64] = wws.create()

    for (ccc, sum) in sizes1.values() do
      wws(ccc) = (sum.f64() / U128.max_value().f64()) + try wws(ccc)? else 0 end
    end
    for (cc, ii) in wws.pairs() do @printf[I32]("@@@weights unit interval ONE@@@: c %s size %.10f\n".cstring(), cc.cstring(), ii*100.0) end ; @printf[I32]("\n".cstring())
    error
else
  None
end
try
    let wws: Map[String, F64] = wws.create()

    for (ccc, sum) in sizes2.values() do
      wws(ccc) = (sum.f64() / U128.max_value().f64()) + try wws(ccc)? else 0 end
    end
    for (cc, ii) in wws.pairs() do @printf[I32]("@@@weights unit interval TWO@@@: c %s size %.10f\n".cstring(), cc.cstring(), ii*100.0) end ; @printf[I32]("\n".cstring())
    error
else
  None
end

    let sizes2b = _coalesce_adjacent_intervals(sizes2)
                          for (c, s) in sizes2b.values() do @printf[I32]("    sizes2b claimant %s size %5.2f%%\n".cstring(), c.cstring(), (s.f64()/U128.max_value().f64())*100.0) end ; @printf[I32]("\n".cstring())
    //// Process additions next.
    let sizes3 = _process_additions(consume sizes2b, size_add, decimal_digits)
                          for (c, s) in sizes3.values() do @printf[I32]("    sizes3 claimant %s size %5.2f%%\n".cstring(), c.cstring(), (s.f64()/U128.max_value().f64())*100.0) end ; @printf[I32]("\n".cstring())
try
    let wws: Map[String, F64] = wws.create()

    for (ccc, sum) in sizes3.values() do
      wws(ccc) = (sum.f64() / U128.max_value().f64()) + try wws(ccc)? else 0 end
    end
    for (cc, ii) in wws.pairs() do @printf[I32]("qqq weights unit interval THREE: c %s size %.10f\n".cstring(), cc.cstring(), ii*100.0) end ; @printf[I32]("\n".cstring())
    error
else
  None
end


    let sizes4 = _coalesce_adjacent_intervals(sizes3)
/*                          for (c, s) in sizes4.values() do @printf[I32]("    sizes4 claimant %s size %5.2f%%\n".cstring(), c.cstring(), (s.f64()/U128.max_value().f64())*100.0) end ; @printf[I32]("\n".cstring())*/

    HashPartitions.create_with_sizes(consume sizes4)

  fun _process_subtractions(old_sizes: Array[(String, U128)],
    size_sub: Map[String, U128]): Array[(String, U128)]
  =>
    let new_sizes: Array[(String, U128)] iso = recover new_sizes.create() end

    try
      for (c, s) in old_sizes.values() do
                          @printf[I32]("_proc_sub: old: c %s size %.2f%%\n".cstring(), c.cstring(), (s.f64()/U128.max_value().f64())*100.0)
        if size_sub.contains(c) then
          let to_sub = size_sub(c)?

          if to_sub > 0 then
            if to_sub >= s then
              new_sizes.push(("", s))     // Unassign all of s
              size_sub(c) = to_sub - s
                          @printf[I32]("_proc_sub: c %s unassign all size %.2f%%\n".cstring(), c.cstring(), (s.f64()/U128.max_value().f64())*100.0)
            else
              let remainder = s - to_sub  // Unassign some of s, keep remainder
              new_sizes.push((c, remainder))
              new_sizes.push(("", to_sub))
              size_sub(c) = 0
                          @printf[I32]("_proc_sub: c %s unassign some, remainder keep size %.2f%%\n".cstring(), c.cstring(), (remainder.f64()/U128.max_value().f64())*100.0)
                          @printf[I32]("_proc_sub: c %s unassign some, unassigned size %.2f%%\n".cstring(), c.cstring(), (to_sub.f64()/U128.max_value().f64())*100.0)
            end
          else
            // c has had enough subtracted from its overall total, keep this
            new_sizes.push((c, s)) // Assignment of s is unchanged
          end
        else
          new_sizes.push((c, s)) // Assignment of s is unchanged
                          @printf[I32]("_proc_sub: c %s unchanged size %.2f%%\n".cstring(), c.cstring(), (s.f64()/U128.max_value().f64())*100.0)
        end
      end
    else
      Fail()
    end
    new_sizes
    
  fun _process_additions(old_sizes: Array[(String, U128)],
    size_add: Map[String, U128], decimal_digits: USize):
    Array[(String, U128)]
  =>
    let new_sizes: Array[(String, U128)] = new_sizes.create()
    let total_length = old_sizes.size()

    try
      for i in Range[USize](0, total_length) do
        (let c, let s) = old_sizes(i)?

        if c != "" then
          new_sizes.push((c, s)) // Assignment of s is unchanged
                              @printf[I32]("proc_add: unchanged at i=%d to %s size %.2f%%\n".cstring(), i, c.cstring(), (s.f64()/U128.max_value().f64())*100.0)
        else
          // Bind neighbors (or dummy values) on the left & right.
          (let left_c, let left_s) = if i > 0 then
            old_sizes(i - 1)?
          else
            ("", 0)
          end
          (let right_c, let right_s) = if i < (total_length - 1) then
            old_sizes(i + 1)?
          else
            ("", 0)
          end

          // Is there a neighbor on the left that needs extra?
          if (i > 0)
            and (left_c != "") and (size_add.contains(left_c))
            and (size_add(left_c)? > 0)
          then
            let to_add = size_add(left_c)?

            if to_add >= s then
              // Assign all of s to left. 
              new_sizes.push((left_c, left_s + s))
              size_add(left_c) = to_add - s
                              @printf[I32]("proc_add: left all assign at i=%d to %s size %.2f%%\n".cstring(), i, left_c.cstring(), ((left_s+s).f64()/U128.max_value().f64())*100.0)
            else
              // Assign some of s, keep remainder unassigned.
              // Split s into 2, copy remaining old_sizes -> new_sizes,
              // then recurse.
              let remainder = s - to_add
              new_sizes.push((left_c, to_add)) // Keep on left side
              new_sizes.push(("", remainder))
              new_sizes.reserve(total_length + 1)
              old_sizes.copy_to(new_sizes, i + 1, i + 2,
                total_length - (i + 1))
              size_add(left_c) = 0
                              @printf[I32]("proc_add: left part assign at i=%d to %s size %.2f%% remainder %.2f%%\n".cstring(), i, left_c.cstring(), (to_add.f64()/U128.max_value().f64())*100.0, (remainder.f64()/U128.max_value().f64())*100.0)
                              for (cc, ss) in new_sizes.values() do @printf[I32]("    claimant %s size %5.2f%%\n".cstring(), cc.cstring(), (ss.f64()/U128.max_value().f64())*100.0) end ; @printf[I32]("Recurse!\n".cstring())
              return _process_additions(new_sizes, size_add, decimal_digits)
            end
          // Is there a neighbor on the right that needs extra?
          elseif (i < (total_length - 1))
            and(right_c != "") and (size_add.contains(right_c))
            and (size_add(right_c)? > 0)
          then
            let to_add = size_add(right_c)?

            if to_add >= s then
              // Assign all of s to right.
              new_sizes.push((right_c, right_s + s))
              size_add(right_c) = to_add - s
                              @printf[I32]("proc_add: right all assign at i=%d to %s size %.2f%%\n".cstring(), i, right_c.cstring(), ((right_s+s).f64()/U128.max_value().f64())*100.0)
            else
              // Assign some of s, keep remainding unassigned.
              // Split s into 2, copy remaining old_sizes -> new_sizes,
              // then recurse.
              let remainder = s - to_add
              new_sizes.push(("", remainder))
              new_sizes.push((right_c, to_add)) // Keep on right side
              new_sizes.reserve(total_length + 1)
              old_sizes.copy_to(new_sizes, i + 1, i + 2,
                total_length - (i + 1))
              size_add(right_c) = 0
                              @printf[I32]("proc_add: right part assign at i=%d to %s remainder %.2f%% size %.2f%%\n".cstring(), i, right_c.cstring(), (remainder.f64()/U128.max_value().f64())*100.0, (to_add.f64()/U128.max_value().f64())*100.0)
                              for (cc, ss) in new_sizes.values() do @printf[I32]("    claimant %s size %5.2f%%\n".cstring(), cc.cstring(), (ss.f64()/U128.max_value().f64())*100.0) end ; @printf[I32]("Recurse!\n".cstring())
              return _process_additions(new_sizes, size_add, decimal_digits)
            end
          // Neither neighbor is suitable, so choose another claimant
          else
                              @printf[I32]("proc_add: NEITHER at i=%d, left=%s, left-size_add=%s, right=%s, right-size_add=%s\n".cstring(), i, left_c.cstring(), try size_add(left_c)?.string().cstring() else "n/a".cstring() end, right_c.cstring(), try size_add(right_c)?.string().cstring() else "n/a".cstring() end)
            let smallest_c = _find_smallest_nonzero_size_to_add(size_add,
              decimal_digits, s)
            if (smallest_c == "") then
                              @printf[I32]("proc_add: NEITHER at i=%d, leave rounding error for final fixup in create2()\n".cstring(), i)
              None // Don't add anything to new_sizes
            else
                              @printf[I32]("proc_add: NEITHER at i=%d, insert zero size for %s.\n".cstring(), i, smallest_c.cstring())
              new_sizes.push((c, s)) // This is the unassigned size
              // Zero size is illegal in the final result, but it will be
              // removed when we recurse, or it will be removed by final
              // neighbor coalescing.
              new_sizes.push((smallest_c, 0))
              new_sizes.reserve(total_length + 1)
              old_sizes.copy_to(new_sizes, i + 1, i + 2,
                total_length - (i + 1))
                              for (cc, ss) in new_sizes.values() do @printf[I32]("    claimant %s size %5.2f%%\n".cstring(), cc.cstring(), (ss.f64()/U128.max_value().f64())*100.0) end ; @printf[I32]("Recurse!\n".cstring())
              return _process_additions(new_sizes, size_add, decimal_digits)
            end
          end
        end // if c != ...
      end //for i ...
    else
      Fail()
    end
    new_sizes

  fun _find_smallest_nonzero_size_to_add(size_add: Map[String,U128],
    decimal_digits: USize, vestige_size: U128): String
  =>
    var smallest_c = ""
    var smallest_s = U128.max_value()

    for (c, s) in size_add.pairs() do
      let qq = (s.f64() / U128.max_value().f64()) * 100.0; @printf[I32]("\tsize_add dump: %s size %.50f%%\n".cstring(), c.cstring(), qq)
      if (s > 0) and (s < smallest_s) then
        smallest_c = c
        smallest_s = s
      end
    end
    if smallest_c != "" then
      smallest_c
    else
      let vestige_perc = (vestige_size.f64() / U128.max_value().f64()) * 100.0
      let vestige_rounded = RoundF64(vestige_perc, decimal_digits + 2)
      if vestige_rounded != 0.0 then
        @printf[I32]("OUCH, vestige_rounded = %.50f%%\n".cstring(), vestige_rounded)
        Fail()
      end
      ""
    end


  fun _coalesce_adjacent_intervals(old_sizes: Array[(String, U128)]):
    Array[(String, U128)] trn^
  =>
    let new_sizes: Array[(String, U128)] trn = recover new_sizes.create() end

    try
      (let first_c, let first_s) = old_sizes.shift()?
      if old_sizes.size() == 0 then
        new_sizes.push((first_c, first_s))
        consume new_sizes
      else
        (let next_c, let next_s) = old_sizes.shift()?
        _coalesce(first_c, first_s, next_c, next_s, old_sizes, consume new_sizes)
      end
    else
      Fail()
      recover Array[(String, U128)]() end
    end

  fun _coalesce(last_c: String, last_s: U128, head_c: String, head_s: U128,
    tail: Array[(String, U128)], new_sizes: Array[(String, U128)] trn):
    Array[(String, U128)] trn^
  =>
    if tail.size() == 0 then
      if last_c == head_c then
        new_sizes.push((last_c, last_s + head_s))
                      try let last = new_sizes(new_sizes.size()-1)?._2.f64() ; @printf[I32]("coalesce: 0a: push claimant %s size %5.2f%%\n".cstring(), last_c.cstring(), (last/U128.max_value().f64())*100.0) else Fail() end
      else
        new_sizes.push((last_c, last_s))
                      try let last = new_sizes(new_sizes.size()-1)?._2.f64() ; @printf[I32]("coalesce: 0b: push claimant %s size %5.2f%%\n".cstring(), last_c.cstring(), (last/U128.max_value().f64())*100.0) else Fail() end
        new_sizes.push((head_c, head_s))
                      try let last = new_sizes(new_sizes.size()-1)?._2.f64() ; @printf[I32]("coalesce: 0b: push claimant %s size %5.2f%%\n".cstring(), last_c.cstring(), (last/U128.max_value().f64())*100.0) else Fail() end
      end
      return consume new_sizes
    end
    try
      (let next_c, let next_s) = tail.shift()?
      if last_c == head_c then
                      @printf[I32]("coalesce: 1a: SAME claimant %s size %5.2f%%\n".cstring(), last_c.cstring(), (head_s.f64()/U128.max_value().f64())*100.0)
        _coalesce(head_c, last_s + head_s, next_c, next_s, tail, consume new_sizes)
      else
        new_sizes.push((last_c, last_s))
                      try let last = new_sizes(new_sizes.size()-1)?._2.f64() ; @printf[I32]("coalesce: 1b: push claimant %s size %5.2f%%\n".cstring(), last_c.cstring(), (last/U128.max_value().f64())*100.0) else Fail() end
        _coalesce(head_c, head_s, next_c, next_s, tail, consume new_sizes)
      end
    else
      Fail()
      recover Array[(String, U128)]() end
    end

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
