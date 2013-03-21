-module(redo_block_tests).
-include_lib("eunit/include/eunit.hrl").

%% copy/pasted
-record(state, {opts=[], refs, acc,
                worker, worker_status=free,
                timers, queue}).

-ifndef(HOST).
-define(HOST, "127.0.0.1").
-endif.

-ifndef(PORT).
-define(PORT, 6379).
-endif.

-ifndef(PASSWORD).
-define(PASSWORD, undefined).
-endif.

-ifndef(DB).
-define(DB, undefined).
-endif.

config() ->
    %% This suite assumes there is a local redis available. Can be defined
    %% through macros.
    [{host, ?HOST}, {port, ?PORT}] ++
     case ?PASSWORD of
         undefined -> [];
         _ -> [{pass, ?PASSWORD}]
     end ++
     case ?DB of
         undefined -> [];
         _ -> [{db, ?DB}]
     end.

start() ->
    application:start(sasl),
    {ok, Pid} = redo_block:start_link(undefined, config()),
    unlink(Pid),
    Pid.

stop(Pid) ->
    redo_block:shutdown(Pid).

seq_test_() ->
    {"Try basic commands in sequence, try to get the right value",
     {setup,
      fun start/0,
      fun stop/1,
      fun(Pid) ->
        Key = lists:flatten(io_lib:format("~p-~p", [make_ref(),now()])),
        Cmds = [redo_block:cmd(Pid, ["EXISTS", Key]),
                redo_block:cmd(Pid, ["SET", Key, "1"]),
                redo_block:cmd(Pid, ["GET", Key]),
                redo_block:cmd(Pid, ["SET", Key, "2"]),
                redo_block:cmd(Pid, ["GET", Key]),
                redo_block:cmd(Pid, ["DEL", Key]),
                redo_block:cmd(Pid, ["EXISTS", Key])],
        ?_assertEqual([0, <<"OK">>, <<"1">>, <<"OK">>, <<"2">>, 1, 0],
                      Cmds)
      end}}.

parallel_test_() ->
    {"Running tests in parallel. They end up being sequential, but all the "
     "results should be in the right order.",
     {setup,
      fun start/0,
      fun stop/1,
      fun(Pid) ->
        Parent = self(),
        Keygen = fun() ->
            iolist_to_binary(io_lib:format("~p-~p", [make_ref(),now()]))
        end,
        %% Takes a unique key, inserts it, then reads it. With hundreds of concurrent
        %% processes, if redo_block doesn't behave well, the val read might be different
        %% from the val written.
        Proc = fun() ->
            Key = Keygen(),
            redo_block:cmd(Pid, ["SET", Key, Key]),
            R = redo_block:cmd(Pid, ["GET", Key]),
            redo_block:cmd(Pid, ["DEL", Key]),
            Parent ! {Key, R}
        end,
        N = 10000,
        _ = [spawn_link(Proc) || _ <- lists:seq(1,N)],
        Res = [receive Msg -> Msg end || _ <- lists:seq(1,N)],
        ?_assertEqual(N, length([ok || {Key,Key} <- Res]))
      end}}.

timeout_test_() ->
    {"A request timeout should kill the connection and start a new one "
     "without necessarily killing follow-up requests or messing up "
     "with the final ordering of responses",
     {setup,
      fun start/0,
      fun stop/1,
      fun(Pid) ->
        Key = lists:flatten(io_lib:format("~p-~p", [make_ref(),now()])),
        Cmds = [redo_block:cmd(Pid, ["EXISTS", Key]),
                redo_block:cmd(Pid, ["SET", Key, "1"]),
                redo_block:cmd(Pid, ["GET", Key], 0),
                redo_block:cmd(Pid, ["SET", Key, "2"]),
                redo_block:cmd(Pid, ["GET", Key]),
                redo_block:cmd(Pid, ["DEL", Key]),
                redo_block:cmd(Pid, ["EXISTS", Key])],
        ?_assertMatch([0, <<"OK">>, _TimeoutOrVal, <<"OK">>, <<"2">>, 1, 0],
                      Cmds)
      end}}.

parallel_timeout_test_() ->
    {"Running tests in parallel. They end up being sequential, but all the "
     "results should be in the right order, even if requests time out all "
     "the time.",
     {setup,
      fun start/0,
      fun stop/1,
      fun(Pid) ->
        Parent = self(),
        Keygen = fun() ->
            iolist_to_binary(io_lib:format("~p-~p", [make_ref(),now()]))
        end,
        %% Takes a unique key, inserts it, then reads it. With hundreds of concurrent
        %% processes, if redo_block doesn't behave well, the val read might be different
        %% from the val written.
        Proc = fun() ->
            Key = Keygen(),
            redo_block:cmd(Pid, ["SET", Key, Key], 30000),
            R1 = redo_block:cmd(Pid, ["GET", Key], 30000),
            R2 = redo_block:cmd(Pid, ["GET", Key], 0),
            R3 = redo_block:cmd(Pid, ["GET", Key], 30000),
            redo_block:cmd(Pid, ["DEL", Key], 60000),
            Parent ! {Key,R1,R2,R3}
        end,
        N = 10000,
        _ = [spawn_link(Proc) || _ <- lists:seq(1,N)],
        Res = [receive Msg -> Msg end || _ <- lists:seq(1,N)],
        ?_assertEqual(N, length([ok || {Key,Key,TimeoutOrVal,Key} <- Res,
                                       TimeoutOrVal == {error,timeout} orelse
                                       TimeoutOrVal == Key]))
      end}}.

timeout_new_conn_test() ->
    Pid = start(),
    {_,_,_,Props1} = sys:get_status(Pid),
    _ = [redo_block:cmd(Pid, ["EXISTS", "1"], 0) || _ <- lists:seq(1,1000)],
    {_,_,_,Props2} = sys:get_status(Pid),
    stop(Pid),
    [#state{worker=Pid1}] = [State || X = [_|_] <- Props1,
                                      {data,[{"State", State}]} <- X],
    [#state{worker=Pid2}] = [State || X = [_|_] <- Props2,
                                      {data,[{"State", State}]} <- X],
    ?assertNotEqual(Pid1, Pid2).
