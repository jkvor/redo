### About

Redo is a pipelined redis client written in Erlang. It lacks any sort of syntactic sugar. The only API function is redo:cmd, which takes a raw redis command.

### Build

    $ make

### Test

    $ ./rebar eunit suite=redo

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
 
