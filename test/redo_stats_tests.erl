
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
-module(redo_stats_tests).
-include_lib("eunit/include/eunit.hrl").

parse_stats_test() ->
    Response = <<"# Server\r\nversion:2.8.19\r\nsha1:00000000\r\n">>,
    ?assertEqual([{<<"Server">>, [{<<"sha1">>, <<"00000000">>}, {<<"version">>, <<"2.8.19">>}]}],
                 redo_stats:parse_response(Response)).

parse_line_test() ->
    ?assertEqual({<<"Server">>, [], []},
                 redo_stats:parse_line(<<"# Server">>, {undefined, [], []})),
    ?assertEqual({undefined, [], [{<<"version">>, <<"2.8.19">>}]},
                 redo_stats:parse_line(<<"version:2.8.19">>, {undefined, [], []})),
    ?assertEqual({undefined, [], [{<<"sha1">>, <<"abcdef">>}, {<<"version">>, <<"2.8.19">>}]},
                 redo_stats:parse_line(<<"sha1:abcdef">>, {undefined, [], [{<<"version">>, <<"2.8.19">>}]})),
    ?assertEqual({undefined, [{<<"Server">>, [{<<"sha1">>, <<"abcdef">>}]}], []},
                 redo_stats:parse_line(<<>>, {<<"Server">>, [], [{<<"sha1">>, <<"abcdef">>}]})).
