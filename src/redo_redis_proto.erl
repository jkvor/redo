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
-module(redo_redis_proto).
-export([package/1, parse/2]).

-define(CRLF, <<"\r\n">>).

%% packet is already a binary
package(Packet) when is_binary(Packet) ->
    [Packet];

%% list of strings - single cmd
package([[Char|_]|_]=Args) when is_integer(Char) ->
    [build_request(Args)];

%% list of binaries - single cmd
package([Bin|_]=Args) when is_binary(Bin) ->
    [build_request(Args)];

%% list of multiple cmds
package(Args) when is_list(Args) ->
    build_pipelined_request(Args, []).

build_request(Args) ->
    Count = length(Args),
    Args1 = [begin
        Arg1 = to_arg(Arg),
        [<<"$">>, integer_to_list(iolist_size(Arg1)), ?CRLF, Arg1, ?CRLF]
     end || Arg <- Args],
    iolist_to_binary(["*", integer_to_list(Count), ?CRLF, Args1, ?CRLF]).

build_pipelined_request([], Acc) ->
    lists:reverse(Acc);

build_pipelined_request([Args|Rest], Acc) ->
    build_pipelined_request(Rest, [build_request(Args)|Acc]).

to_arg(List) when is_list(List) ->
    List;

to_arg(Bin) when is_binary(Bin) ->
    Bin;

to_arg(Int) when is_integer(Int) ->
    integer_to_list(Int);

to_arg(Atom) when is_atom(Atom) ->
    atom_to_list(Atom).

%% Single line reply
parse(Callback, {raw, <<"+", Rest/binary>> = Data}) ->
    case read_line(Rest) of
        {ok, Str, Rest1} ->
            Callback(Str),
            {ok, {raw, Rest1}};
        {error, eof} ->
            {eof, {raw, Data}}
    end;

%% Error msg reply
parse(Callback, {raw, <<"-", Rest/binary>> = Data}) ->
    case read_line(Rest) of
        {ok, Err, Rest1} ->
            Callback({error, Err}),
            {ok, {raw, Rest1}};
        {error, eof} ->
            {eof, {raw, Data}}
    end;

%% Integer reply
parse(Callback, {raw, <<":", Rest/binary>> = Data}) ->
    case read_line(Rest) of
        {ok, Int, Rest1} ->
            Callback(list_to_integer(binary_to_list(Int))),
            {ok, {raw, Rest1}};
        {error, eof} ->
            {eof, {raw, Data}}
    end;

%% Bulk reply
parse(Callback, {raw, <<"$", Rest/binary>> = Data}) ->
    case read_line(Rest) of
        {ok, BinSize, Rest1} ->
            Size = list_to_integer(binary_to_list(BinSize)),
            case Size >= 0 of
                true ->
                    case Rest1 of
                        <<Str:Size/binary, "\r\n", Rest2/binary>> ->
                            Callback(Str),
                            {ok, {raw, Rest2}};
                        _ ->
                            {eof, {raw, Data}}
                    end;
                false ->
                    Callback(undefined),
                    {ok, {raw, Rest1}}
            end;
        {error, eof} ->
            {eof, {raw, Data}}
    end;

%% Multi bulk reply
parse(Callback, {raw, <<"*", Rest/binary>> = Data}) ->
    case read_line(Rest) of
        {ok, BinNum, Rest1} ->
            Num = list_to_integer(binary_to_list(BinNum)),
            Callback({multi_bulk, Num}),
            parse(Callback, {multi_bulk, Num, Rest1});
        {error, eof} ->
            {eof, {raw, Data}}
    end;

parse(Callback, {multi_bulk, Num, Data}) ->
    multi_bulk(Callback, Num, Data).

read_line(Data) ->
    read_line(Data, <<>>).

read_line(<<"\r\n", Rest/binary>>, Acc) ->
    {ok, Acc, Rest};

read_line(<<>>, _Acc) ->
    {error, eof};

read_line(<<C, Rest/binary>>, Acc) ->
    read_line(Rest, <<Acc/binary, C>>).

multi_bulk(_Callback, 0, Rest) ->
    {ok, {raw, Rest}};

multi_bulk(_Callback, Num, <<>>) ->
    {eof, {multi_bulk, Num, <<>>}};

multi_bulk(Callback, Num, Rest) ->
    case parse(Callback, {raw, Rest}) of
        {ok, {raw, Rest1}} ->
            multi_bulk(Callback, Num-1, Rest1);
        {eof, _} ->
            {eof, {multi_bulk, Num, Rest}}
    end.

