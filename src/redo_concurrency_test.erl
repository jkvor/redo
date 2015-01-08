%% Copyright (c) 2011 Jacob Vorreuter <jacob.vorreuter@gmail.com>
%% 
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%% 
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%% 
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
-module(redo_concurrency_test).
-export([run/2]).

run(NumPids, NumOps) ->
    io:format("================================================================================~n"),
    io:format("== THIS TEST RELIES ON A LOCAL REDIS SERVER RUNNING ON localhost:6379         ==~n"),
    io:format("================================================================================~n"),

    case whereis(redo) of
        undefined ->
            {ok, _Pid} = redo:start_link();
        _ ->
            ok
    end,

    <<"OK">> = redo:cmd(["SELECT", "6"]),
    <<"OK">> = redo:cmd(["FLUSHDB"]),

    Self = self(),
    Pids = [spawn_link(fun() -> io:format("spawned ~w~n", [N]), worker(Self, N, NumOps) end) || N <- lists:seq(1, NumPids)],

    [receive {Pid, N, done} -> io:format("finished ~w~n", [N]) end || Pid <- Pids],

    ok.

worker(Parent, N, 0) ->
    Parent ! {self(), N, done};

worker(Parent, N, NumOps) ->
    random:seed(now()),
    StrN = integer_to_list(N),
    StrOp = integer_to_list(NumOps),
    case random:uniform(100) of
        R when R > 0, R =< 24 ->
            Key = iolist_to_binary([StrN, ":", StrOp, ":STRING"]),
            Val = iolist_to_binary(["STRING:", StrN, ":", StrOp]),
            <<"OK">> = redo:cmd(["SET", Key, Val]),
            Val = redo:cmd(["GET", Key]);
        R when R > 24, R =< 48 ->
            Key = iolist_to_binary([StrN, ":", StrOp, ":SET"]),
            Val1 = iolist_to_binary(["SET:1:", StrN, ":", StrOp]),
            Val2 = iolist_to_binary(["SET:2:", StrN, ":", StrOp]),
            1 = redo:cmd(["SADD", Key, Val1]),
            1 = redo:cmd(["SADD", Key, Val2]),
            Set = redo:cmd(["SMEMBERS", Key]),
            true = lists:member(Val1, Set),
            true = lists:member(Val2, Set);
        R when R > 48, R =< 72 ->
            Key = iolist_to_binary([StrN, ":", StrOp, ":HASH"]),
            Vals = [iolist_to_binary(["HASH:1:", StrN, ":Key:", StrOp]),
                    iolist_to_binary(["HASH:1:", StrN, ":Val:", StrOp]),
                    iolist_to_binary(["HASH:2:", StrN, ":Key", StrOp]),
                    iolist_to_binary(["HASH:2:", StrN, ":Val", StrOp])],
            <<"OK">> = redo:cmd(["HMSET", Key | Vals]),
            Vals = redo:cmd(["HGETALL", Key]);
        R when R > 72, R =< 98 ->
            Key1 = iolist_to_binary([StrN, ":", StrOp, ":PIPESET"]),
            Val1 = iolist_to_binary(["PIPESET:1:", StrN, ":", StrOp]),
            Val2 = iolist_to_binary(["PIPESET:2:", StrN, ":", StrOp]),
            1 = redo:cmd(["SADD", Key1, Val1]),
            1 = redo:cmd(["SADD", Key1, Val2]),
            Key2 = iolist_to_binary([StrN, ":", StrOp, ":PIPEHASH"]),
            Vals = [iolist_to_binary(["PIPEHASH:1:", StrN, ":Key:", StrOp]),
                    iolist_to_binary(["PIPEHASH:1:", StrN, ":Val:", StrOp]),
                    iolist_to_binary(["PIPEHASH:2:", StrN, ":Key", StrOp]),
                    iolist_to_binary(["PIPEHASH:2:", StrN, ":Val", StrOp])],
            <<"OK">> = redo:cmd(["HMSET", Key2 | Vals]),
            [Set, Hash] = redo:cmd([["SMEMBERS", Key1], ["HGETALL", Key2]]),
            true = lists:member(Val1, Set),
            true = lists:member(Val2, Set),
            Vals = Hash;
        R when R > 98 ->
            Keys = [begin
                Key = iolist_to_binary([StrN, ":", StrOp, ":", string:right(integer_to_list(ItemNum), 200, $0)]),
                <<"OK">> = redo:cmd(["SET", Key, "0"]),
                Key
            end || ItemNum <- lists:seq(1,1000)],
            Keys1 = redo:cmd(["KEYS", iolist_to_binary([StrN, ":", StrOp, ":*"])]),
            L = length(Keys),
            L1 = length(Keys1),
            L = L1
    end,
    worker(Parent, N, NumOps-1).

