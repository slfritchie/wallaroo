use "collections"
use "crypto"
use "wallaroo_labs/mort"

class ref HashPartitions
  let lower_bounds: Array[U128]
  let sizes: Array[U128] = sizes.create()
  let nodes: Map[U128, String] = nodes.create()

  new ref create(nodes': Array[String] val) =>
    lower_bounds = Array[U128]
    let count = nodes'.size()
    let part_size = U128.max_value() / count.u128()
    var next_lower_bound: U128 = 0
    for i in Range[USize](0, count) do
      lower_bounds.push(next_lower_bound)
      sizes.push(part_size)
      try
        nodes(next_lower_bound) = nodes'(i)?
      else
        Fail()
      end
      next_lower_bound = next_lower_bound + part_size
    end

    var sum: U128 = 0
    for s in sizes.values() do
      sum = sum + s
    end
    let idx = lower_bounds.size() - 1
    let adjust = (U128.max_value() - sum)
    try sizes(idx)? = sizes(idx)? + adjust else Fail() end


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

  fun pretty_print() =>
    var min_size: U128 = U128.max_value()

    try
      for i in Range[USize](0, sizes.size()) do
        min_size = min_size.min(sizes(i)?)
      end

      for i in Range[USize](0, sizes.size()) do
        @printf[I32]("node %10s relative-size %.1f\n".cstring(), nodes(lower_bounds(i)?)?.cstring(), sizes(i)?.f64() / min_size.f64())
      end
    end

  fun ref twiddle(from: String, to: String) =>
    for (lb, s) in nodes.pairs() do
      if s == from then
        nodes(lb) = to
        return
      end
    end
