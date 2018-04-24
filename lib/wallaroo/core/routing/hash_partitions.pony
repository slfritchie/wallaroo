use "collections"
use "crypto"
use "wallaroo_labs/mort"

class val HashPartitions
  let lower_bounds: Array[U128]
  let sizes: Array[U128] = sizes.create()
  let nodes: Map[U128, String] = nodes.create()

  new val create(nodes': Array[String] val) =>
    lower_bounds = Array[U128]
    let count = nodes'.size().u128()
    let part_size = U128.max_value() / count
    var next_lower_bound: U128 = 0
    for i in Range[U128](0, count) do
      lower_bounds.push(next_lower_bound)
      sizes.push(part_size)
      try
        nodes(next_lower_bound) = nodes'(i.usize())?
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
    @printf[I32]("\tadjust = %s\n".cstring(), adjust.string().cstring())
    try sizes(idx)? = sizes(idx)? + adjust end


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
        let diff = try
          let lb = lower_bounds(i)?    // inclusive
          let ub = lower_bounds(i+1)?  // exclusive
          @printf[I32]("\t** i=%d lb=%s ub=%s\n".cstring(), i, lb.string().cstring(), ub.string().cstring())
          lower_bounds(i+1)? - lower_bounds(i)?
        else
          @printf[I32]("\t** i=%d lb=%s\n".cstring(), i, lower_bounds(i)?.string().cstring())
          (U128.max_value() - lower_bounds(i)?) + 1
        end
        w(node) = w(node)? + diff
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
