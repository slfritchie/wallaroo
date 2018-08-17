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

use "options"
use "wallaroo"
use "wallaroo/core/source"

primitive TCPSourceConfigCLIParser
  fun apply(args: Array[String] val): Array[TCPSourceConfigOptions] val ? =>
@printf[I32]("SLF: hey 99\n".cstring())
    for yyy in args.values() do
      @printf[I32]("SLF: hey 99, args = %s\n".cstring(), yyy.cstring())
    end
    let in_arg = "in"
    let short_in_arg = "i"

    let options = Options(args, false)

    options.add(in_arg, short_in_arg, StringArgument, Required)
    options.add("help", None)

    for option in options do
      match option
      | ("help", let arg: None) =>
        StartupHelp()
      | (in_arg, let input: String) =>
        return _from_input_string(input)?
      end
    end

    error

  fun _from_input_string(inputs: String): Array[TCPSourceConfigOptions] val ? =>
    let opts = recover trn Array[TCPSourceConfigOptions] end

    for input in inputs.split(",").values() do
      let i = input.split(":")
      opts.push(TCPSourceConfigOptions(i(0)?, i(1)?))
    end

    consume opts

class val TCPSourceConfigOptions
  let host: String
  let service: String

  new val create(host': String, service': String) =>
    host = host'
    service = service'

class val TCPSourceConfig[In: Any val]
  let _handler: FramedSourceHandler[In] val
  let _host: String
  let _service: String

  new val create(handler': FramedSourceHandler[In] val, host': String, service': String) =>
    _handler = handler'
    _host = host'
    _service = service'

  new val from_options(handler': FramedSourceHandler[In] val, opts: TCPSourceConfigOptions) =>
    _handler = handler'
    _host = opts.host
    _service = opts.service

  fun source_listener_builder_builder(): TCPSourceListenerBuilderBuilder =>
    TCPSourceListenerBuilderBuilder(_host, _service)

  fun source_builder(app_name: String, name: String):
    TypedTCPSourceBuilderBuilder[In]
  =>
    TypedTCPSourceBuilderBuilder[In](app_name, name, _handler, _host, _service)
