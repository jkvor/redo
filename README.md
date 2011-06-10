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

### Run

    $ erl -pa ebin
    1> redo:start_link([{host, "localhost"}, {port, 6379}]).
    {ok,<0.33.0>}
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
    7> redo:cmd([["GET", "foo"], ["HGETALL", "hfoo"]]).
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
 

