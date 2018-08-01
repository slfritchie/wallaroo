

use "promises"
use "wallaroo/core/source"
use "wallaroo_labs/mort"


trait _BarrierInitiatorPhase
  fun name(): String

  fun ref initiate_barrier(barrier_token: BarrierToken,
    result_promise: BarrierResultPromise)
  =>
    _invalid_call()

  fun ref source_registration_complete(s: Source) =>
    _invalid_call()

  fun ready_for_next_token(): Bool =>
    false

  fun higher_priority(t: BarrierToken): Bool =>
    """
    If this is called on any phase other than RollbackBarrierInitiatorPhase,
    then this is the only rollback token being processed and it is thus
    higher priority than any other tokens in progress.
    """
    match t
    | let srt: SnapshotRollbackBarrierToken =>
      true
    else
      false
    end

  fun ref ack_barrier(s: BarrierReceiver, barrier_token: BarrierToken,
    active_barriers: ActiveBarriers)
  =>
    active_barriers.ack_barrier(s, barrier_token)

  fun ref worker_ack_barrier_start(w: String, barrier_token: BarrierToken,
    active_barriers: ActiveBarriers)
  =>
    active_barriers.worker_ack_barrier_start(w, barrier_token)

  fun ref worker_ack_barrier(w: String, barrier_token: BarrierToken,
    active_barriers: ActiveBarriers)
  =>
    active_barriers.worker_ack_barrier(w, barrier_token)

  fun ref barrier_complete(token: BarrierToken) =>
    _invalid_call()

  fun _invalid_call() =>
    @printf[I32]("Invalid call on barrier initiator phase %s\n".cstring(),
      name().cstring())
    Fail()

class _InitialBarrierInitiatorPhase is _BarrierInitiatorPhase
  fun name(): String => "_InitialBarrierInitiatorPhase"

class _NormalBarrierInitiatorPhase is _BarrierInitiatorPhase
  let _initiator: BarrierInitiator ref

  new create(initiator: BarrierInitiator ref) =>
    _initiator = initiator

  fun name(): String =>
    "_NormalBarrierInitiatorPhase"

  fun ref initiate_barrier(barrier_token: BarrierToken,
    result_promise: BarrierResultPromise)
  =>
    _initiator.initiate_barrier(barrier_token, result_promise)

  fun ready_for_next_token(): Bool =>
    true

  fun ref barrier_complete(token: BarrierToken) =>
    _initiator.next_token()

class _SourcePendingBarrierInitiatorPhase is _BarrierInitiatorPhase
  """
  In this phase, we queue new barriers and wait to inject them until after
  the pending source has registered with its downstreams.
  """
  let _initiator: BarrierInitiator ref

  new create(initiator: BarrierInitiator ref) =>
    _initiator = initiator

  fun name(): String =>
    "_SourcePendingBarrierInitiatorPhase"

  fun ref initiate_barrier(barrier_token: BarrierToken,
    result_promise: BarrierResultPromise)
  =>
    _initiator.queue_barrier(barrier_token, result_promise)

  fun ref source_registration_complete(s: Source) =>
    _initiator.source_pending_complete(s)

class _BlockingBarrierInitiatorPhase is _BarrierInitiatorPhase
  let _initiator: BarrierInitiator ref
  let _initial_token: BarrierToken
  let _wait_for_token: BarrierToken

  new create(initiator: BarrierInitiator ref, token: BarrierToken,
    wait_for_token: BarrierToken)
  =>
    _initiator = initiator
    _initial_token = token
    _wait_for_token = wait_for_token

  fun name(): String =>
    "_BlockingBarrierInitiatorPhase"

  fun ref initiate_barrier(barrier_token: BarrierToken,
    result_promise: BarrierResultPromise)
  =>
    if (barrier_token == _initial_token) or
      (barrier_token == _wait_for_token)
    then
      @printf[I32]("!@ BlockPhase: Initiating barrier %s!\n".cstring(), barrier_token.string().cstring())
      _initiator.initiate_barrier(barrier_token, result_promise)
    else
      @printf[I32]("!@ BlockPhase: Queuing barrier %s!\n".cstring(), barrier_token.string().cstring())
      _initiator.queue_barrier(barrier_token, result_promise)
    end

  fun ref barrier_complete(token: BarrierToken) =>
    if token == _wait_for_token then
      _initiator.next_token()
    end

class _RollbackBarrierInitiatorPhase is _BarrierInitiatorPhase
  let _initiator: BarrierInitiator ref
  let _token: BarrierToken

  new create(initiator: BarrierInitiator ref, token: BarrierToken) =>
    _initiator = initiator
    _token = token

  fun name(): String =>
    "_RollbackBarrierInitiatorPhase"

  fun higher_priority(t: BarrierToken): Bool =>
    t > _token

  fun ref initiate_barrier(barrier_token: BarrierToken,
    result_promise: BarrierResultPromise)
  =>
    if (barrier_token == _token) then
      _initiator.initiate_barrier(barrier_token, result_promise)
    else
      // !@ TODO: Is it safe to queue barriers that arrive after the rollback
      // token? Or should we be dropping all barriers until rollback is
      // complete?
      _initiator.queue_barrier(barrier_token, result_promise)
    end

  fun ref barrier_complete(token: BarrierToken) =>
    """
    We are clearing all old barrier info, so we ignore this.
    """
    None

  fun ref rollback_barrier_complete(token: BarrierToken) =>
    if token == _token then
      _initiator.next_token()
    end

  ////////////////////////////
  // Managing active barriers
  //
  // We drop any barrier-related activity unless it's related to our
  // current rollback token. By the time the rollback token is acked at
  // all sinks, we know that no more past activity will arrive.
  /////////////////////////////
  fun ref ack_barrier(s: BarrierReceiver, barrier_token: BarrierToken,
    active_barriers: ActiveBarriers)
  =>
    if barrier_token == _token then
      active_barriers.ack_barrier(s, barrier_token)
    end

  fun ref worker_ack_barrier_start(w: String, barrier_token: BarrierToken,
    active_barriers: ActiveBarriers)
  =>
    if barrier_token == _token then
      active_barriers.worker_ack_barrier_start(w, barrier_token)
    end

  fun ref worker_ack_barrier(w: String, barrier_token: BarrierToken,
    active_barriers: ActiveBarriers)
  =>
    if barrier_token == _token then
      active_barriers.worker_ack_barrier(w, barrier_token)
    end
