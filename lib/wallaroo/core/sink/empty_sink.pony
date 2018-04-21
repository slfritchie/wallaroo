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
use "wallaroo/core/boundary"
use "wallaroo/core/common"
use "wallaroo/ent/data_receiver"
use "wallaroo/ent/router_registry"
use "wallaroo/core/initialization"
use "wallaroo/core/routing"
use "wallaroo/core/topology"
use "wallaroo_labs/mort"

actor EmptySink is Consumer
  be run[D: Any val](metric_name: String, pipeline_time_spent: U64, data: D,
    i_producer_id: StepId, i_producer: Producer, msg_uid: MsgId,
    frac_ids: FractionalMessageId, i_seq_id: SeqId, i_route_id: RouteId,
    latest_ts: U64, metrics_id: U16, worker_ingress_ts: U64)
  =>
    ifdef "trace" then
      @printf[I32]("Rcvd msg at EmptySink\n".cstring())
    end
    None

  be replay_run[D: Any val](metric_name: String, pipeline_time_spent: U64,
    data: D, producer_id: StepId, producer: Producer, msg_uid: MsgId,
    frac_ids: FractionalMessageId, incoming_seq_id: SeqId, route_id: RouteId,
    latest_ts: U64, metrics_id: U16, worker_ingress_ts: U64)
  =>
    None

  be application_begin_reporting(initializer: LocalTopologyInitializer) =>
    initializer.report_created(this)

  be application_created(initializer: LocalTopologyInitializer,
    omni_router: OmniRouter)
  =>
    initializer.report_initialized(this)

  be application_initialized(initializer: LocalTopologyInitializer,
    router_registry: RouterRegistry) =>
    initializer.report_ready_to_work(this)

  be application_ready_to_work(initializer: LocalTopologyInitializer) =>
    None

  be register_producer(producer: Producer) =>
    None

  be unregister_producer(producer: Producer) =>
    None

  be report_status(code: ReportStatusCode) =>
    None

  be request_in_flight_ack(request_id: RequestId, requester_id: StepId,
    producer: InFlightAckRequester)
  =>
    producer.receive_in_flight_ack(request_id)

  be request_in_flight_resume_ack(in_flight_resume_ack_id: InFlightResumeAckId,
    request_id: RequestId, requester_id: StepId,
    requester: InFlightAckRequester, leaving_workers: Array[String] val)
  =>
    None

  be try_finish_in_flight_request_early(requester_id: StepId) =>
    None

  be request_ack() =>
    None

  be receive_state(state: ByteSeq val) => Fail()

  be dispose() =>
    None
