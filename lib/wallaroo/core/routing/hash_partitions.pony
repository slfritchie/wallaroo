use "collections"
use "crypto"
use "wallaroo_labs/mort"

class val HashPartitions
  let lower_bounds: Array[U128]
  let nodes: Map[U128, String] = nodes.create()

  new val create(nodes': Array[String] val) =>
    lower_bounds = Array[U128]
    let count = nodes'.size().u128()
    let part_size = U128.max_value() / count
    var next_lower_bound: U128 = 0
    for i in Range[U128](0, count) do
      lower_bounds.push(next_lower_bound)
      try
        nodes(next_lower_bound) = nodes'(i.usize())?
      else
        @printf[I32]("What went wrong?\n".cstring())
      end
      next_lower_bound = next_lower_bound + part_size
  end


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
          @printf[I32]("\t** i=%d hb=0x%llx lb=0x%llx\n".cstring(), i, lower_bounds(i+1)?, lower_bounds(i)?)
          lower_bounds(i+1)? - lower_bounds(i)?
        else
          @printf[I32]("\t** i=%d lb=0x%llx\n".cstring(), i, lower_bounds(i)?)
          (U128.max_value() - lower_bounds(i)?) + 1
        end
        w(node) = w(node)? + diff
        /****
        let upper_bound = try     // inclusive upper bound
          lower_bounds(i+1)?
        else
          U128.max_value()
        end
        let node = nodes(lower_bounds(i)?)?
        w(node) = w(node)? + (upper_bound - lower_bounds(i)?)
         ****/
      end
    else
      Fail()
    end
    w