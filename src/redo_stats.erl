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
-module(redo_stats).

-export([get_stats/1]).

-ifdef(TEST).
-compile(export_all).
-endif.

%% Fetch the stats from Redis and return a proplist of values.
-spec get_stats(atom() | pid()) -> [{term(), binary()}] | {'error', Reason::term()}.
get_stats(Conn) ->
    parse_response(redo:cmd(Conn, get_stats_cmd())).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

get_stats_cmd() ->
    [<<"INFO">>, <<"default">>].

parse_response(Response) ->
    Lines = re:split(Response, <<"\r\n">>),
    {undefined, Sections, []} = lists:foldl(fun parse_line/2, {undefined, [], []}, Lines),
    Sections.

parse_line(<<>>, {Section, AllSections, CurrentSection}) ->
    {undefined, [{Section, CurrentSection}] ++ AllSections, []};

parse_line(<<"# ", Section/binary>>, {undefined, AllSections, CurrentSection}) ->
    {Section, AllSections, CurrentSection};

parse_line(Line, {Section, AllSections, CurrentSection}) ->
    [Key, Value] = binary:split(Line, <<":">>),
    {Section, AllSections, [{Key, Value}] ++ CurrentSection}.
