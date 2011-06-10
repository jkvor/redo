### Philosophy

> If you wish to write a redis client from scratch, you must first invent the universe.

### About

Redo is a pipelined redis client written in Erlang. It lacks any sort of syntactic sugar. The only API function is redo:cmd, which takes a raw redis command.

### Build

    $ make

### Test

#### Unit tests

    $ ./rebar eunit suite=redo

#### Local read benchmark

    $ erl -pa ebin
    1> bench:sync(1000).
    91ms
    10989 req/sec

    2> bench:async(1000, 100).
    38ms
    26315 req/sec

#### Concurrency test

    $ erl -pa ebin
    1> redo_concurrency_test:run(20, 100). %% 20 pids, 100 random operations performed per pid

### Start

No arguments: register process as "redo"

    $ erl -pa ebin
    1> redo:start_link().
    {ok, <0.33.0>
    2> whereis(redo).
    <0.33.0>
    3> redo:cmd(["PING"]).  
    <<"PONG">>

Register with custom name

    erl -pa ebin
    1> redo:start_link(myclient).
    {ok,<0.33.0>}
    2> whereis(myclient).
    <0.33.0>
    8> redo:cmd(myclient, ["PING"]).  
    <<"PONG">>

Start anonymous Redo process

    erl -pa ebin
    1> {ok, Pid} = redo:start_link(undefined).
    {ok,<0.33.0>}
    2> redo:cmd(Pid, ["PING"]).
    <<"PONG">>

Specifying connection options

    erl -pa ebin
    1> redo:start_link([{host, "localhost"}, {port, 6379}]).
    {ok,<0.33.0>}
    2> redo:cmd(["PING"]).
    <<"PONG">>

    3> redo:start_link(myclient, [{host, "localhost"}, {port, 6379}]).
    {ok,<0.37.0>}
    4> redo:cmd(myclient, ["PING"]).
    <<"PONG">>

### Commands

    erl -pa ebin
    1> redo:start_link().
    <0.33.0>
    2> redo:cmd(["SET", "foo"]).
    {error,<<"ERR wrong number of arguments for 'set' command">>}
    3> redo:cmd(["SET", "foo", "bar"]).
    <<"OK">>
    4> redo:cmd(["GET", "foo"]).
    <<"bar">>
    5> redo:cmd(["HMSET", "hfoo", "ONE", "ABC", "TWO", "DEF"]).
    <<"OK">>
    6> redo:cmd(["HGETALL", "hfoo"]).
    [<<"ONE">>,<<"ABC">>,<<"TWO">>,<<"DEF">>]

### Pipelined commands

    1> redo:start_link().
    <0.33.>
    2> redo:cmd([["GET", "foo"], ["HGETALL", "hfoo"]]).
    [<<"bar">>, [<<"ONE">>,<<"ABC">>,<<"TWO">>,<<"DEF">>]]

### Pub/Sub

    $ erl -pa ebin
    1> redo:start_link().
    {ok,<0.33.0>}
    2> Ref = redo:subscribe("chfoo").
    #Ref<0.0.0.42>
    3> (fun() -> receive {Ref, Res} -> Res after 10000 -> timeout end end)().
    [<<"subscribe">>,<<"chfoo">>,1]
    4> redo:start_link(client).  
    {ok,<0.39.0>}
    5> redo:cmd(client, ["PUBLISH", "chfoo", "hello"]).
    1
    6> (fun() -> receive {Ref, Res} -> Res after 10000 -> timeout end end)().
    [<<"message">>,<<"chfoo">>,<<"hello">>]

    %% restart redis server...

    7> (fun() -> receive {Ref, Res} -> Res after 10000 -> timeout end end)().
    closed
    8> f(Ref).
    ok
    9> Ref = redo:subscribe("chfoo").                                        
    #Ref<0.0.0.68>
    10> (fun() -> receive {Ref, Res} -> Res after 10000 -> timeout end end)().
    [<<"subscribe">>,<<"chfoo">>,1]
    11> redo:cmd(client, ["PUBLISH", "chfoo", "hello again"]).
    1
    12> (fun() -> receive {Ref, Res} -> Res after 10000 -> timeout end end)().
    [<<"message">>,<<"chfoo">>,<<"hello again">>]
