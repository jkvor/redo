-module(bench).
-compile(export_all).

sync(Num) ->
    Redo = setup(),
    A = now(),
    ok = loop(Redo, Num),
    B = now(),
    print(Num,A,B),
    ok.

async(Num, Concurrency) ->
    Redo = setup(),
    Self = self(),
    A = now(),
    Pids = [spawn_link(fun() -> loop(Redo, Num div Concurrency), Self ! {self(), done} end) || _ <- lists:seq(1, Concurrency)],
    [receive {Pid, done} -> ok end || Pid <- Pids],
    B = now(),
    print(Num,A,B), 
    ok.

setup() ->
    {ok, Redo} = redo:start_link(undefined, []),
    redo:cmd(Redo, ["SET", "foo", "bar"]),
    Redo.

print(Num,A,B) ->
    Microsecs = timer:now_diff(B,A),
    Time = Microsecs div Num,
    PerSec = 1000000 div Time,
    io:format("~wms~n~w req/sec~n", [Microsecs div 1000, PerSec]).
 
loop(_Pid, 0) -> ok;

loop(Pid, Count) ->
    <<"bar">> = redo:cmd(Pid, ["GET", "foo"]),
    loop(Pid, Count-1).
