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
-module(redo_tests).
-include_lib("eunit/include/eunit.hrl").

redis_uri_test() ->
    ?assertEqual([{host, "127.0.0.1"},
                  {port, 666},
                  {pass, "password"},
                  {db, "1"}],
                 redo_uri:parse("redis://:password@127.0.0.1:666/1")),

    ?assertEqual([{host, "127.0.0.1"},
                  {port, 666},
                  {pass, "password"},
                  {db, "1"}],
                 redo_uri:parse("redis://password@127.0.0.1:666/1")),

    ?assertEqual([{host, "127.0.0.1"},
                  {pass, "password"}],
                 redo_uri:parse("redis://password@127.0.0.1")),

    ?assertEqual([{host, "127.0.0.1"},
                  {port, 666},
                  {db, "1"}],
                 redo_uri:parse("redis://127.0.0.1:666/1")),

    ?assertEqual([{host, "127.0.0.1"},
                  {db, "1"}],
                 redo_uri:parse("redis://127.0.0.1/1")),

    ?assertEqual([{host, "127.0.0.1"},
                  {port, 666}],
                 redo_uri:parse("redis://127.0.0.1:666")),

    ?assertEqual([{host, "127.0.0.1"}], redo_uri:parse("redis://127.0.0.1")),

    ?assertEqual([], redo_uri:parse("redis://")),

    ?assertEqual([], redo_uri:parse("")),

    ok.

redis_proto_test() ->
    redo_redis_proto:parse(
        fun(Val) -> ?assertEqual(undefined, Val) end,
        {raw, <<"$-1\r\n">>}),

    redo_redis_proto:parse(
        fun(Val) -> ?assertEqual(<<>>, Val) end,
        {raw, <<"$0\r\n\r\n">>}),

    redo_redis_proto:parse(
        fun(Val) -> ?assertEqual(<<"OK">>, Val) end,
        {raw, <<"+OK\r\n">>}),

    redo_redis_proto:parse(
        fun(Val) -> ?assertEqual({error, <<"Error">>}, Val) end,
        {raw, <<"-Error\r\n">>}),
 
    redo_redis_proto:parse(
        fun(Val) -> ?assertEqual(<<"FOO">>, Val) end,
        {raw, <<"$3\r\nFOO\r\n">>}),

    redo_redis_proto:parse(
        fun(Val) -> ?assertEqual(1234, Val) end,
        {raw, <<":1234\r\n">>}),

    ok.
