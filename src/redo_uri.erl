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
-module(redo_uri).
-export([parse/1]).

-spec parse(binary() | list()) -> list().
parse(Url) when is_binary(Url) ->
    parse(binary_to_list(Url));

parse("redis://" ++ Rest) ->
    parse(Rest);

parse(":" ++ Rest) ->
    parse(Rest);

% redis://:password@localhost:6379/1
parse(Url) when is_list(Url) ->
    {Pass, Rest1} = pass(Url),
    {Host, Rest2} = host(Rest1),
    {Port, Db} = port(Rest2),
    [{host, Host} || Host =/= ""] ++
    [{port, Port} || Port =/= ""] ++
    [{pass, Pass} || Pass =/= ""] ++
    [{db, Db}     || Db   =/= ""].

pass("") ->
    {"", ""};

pass(Url) ->
    case string:tokens(Url, "@") of
        [Rest] -> {"", Rest};
        [Pass, Rest] -> {Pass, Rest};
        [Pass | Rest] -> {Pass, string:join(Rest, "@")}
    end.

host("") ->
    {"", ""};

host(Url) ->
    case string:tokens(Url, ":") of
        [Rest] ->
            case string:tokens(Rest, "/") of
                [Host] -> {Host, ""};
                [Host, Rest1] -> {Host, "/" ++ Rest1};
                [Host | Rest1] -> {Host, string:join(Rest1, "/")}
            end;
        [Host, Rest] -> {Host, Rest};
        [Host | Rest] -> {Host, string:join(Rest, ":")}
    end.

port("") ->
    {"", ""};

port("/" ++ Rest) ->
    {"", Rest};

port(Url) ->
    case string:tokens(Url, "/") of
        [Port] -> {list_to_integer(Port), ""};
        [Port, Rest] -> {list_to_integer(Port), Rest};
        [Port | Rest] -> {list_to_integer(Port), string:join(Rest, "/")}
    end.
