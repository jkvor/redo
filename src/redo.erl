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
-module(redo).
-behaviour(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/0, start_link/1, start_link/2, 
         cmd/1, cmd/2, cmd/3]).

-record(state, {host, port, pass, sock, queue, cancelled, buffer}).

-define(TIMEOUT, 30000).

start_link() ->
    start_link([]).

start_link(Opts) when is_list(Opts) ->
    start_link(?MODULE, Opts).

start_link(undefined, Opts) when is_list(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []);

start_link(Name, Opts) when is_atom(Name), is_list(Opts) ->
    gen_server:start_link({local, Name}, ?MODULE, [Opts], []).

cmd(Cmd) ->
    cmd(?MODULE, Cmd, ?TIMEOUT).

cmd(NameOrPid, Cmd) ->
    cmd(NameOrPid, Cmd, ?TIMEOUT).

cmd(NameOrPid, Cmd, Timeout) when is_integer(Timeout) ->
    Packets = redo_redis_proto:package(Cmd),
    Ref = gen_server:call(NameOrPid, {cmd, Packets}, 2000),
    case length(Packets) of
        1 ->
            receive_resp(NameOrPid, Cmd, Ref, Timeout, []);
        Len ->
            [receive_resp(NameOrPid, Cmd, Ref, Timeout, []) || _ <- lists:seq(1,Len)]
    end.

receive_resp(NameOrPid, Cmd, Ref, Timeout, Acc) ->
    receive
        {Ref, done} ->
            case Acc of
                [Val] -> Val;
                _ -> lists:reverse(Acc)
            end;
        {Ref, closed} when length(Acc) == 0 ->
            cmd(NameOrPid, Cmd, Timeout);
        {Ref, Val} ->
            receive_resp(NameOrPid, Cmd, Ref, Timeout, [Val|Acc])
    after Timeout ->
            gen_server:cast(NameOrPid, {cancel, Ref}),
            {error, timeout}
    end.
    
%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%    {ok, State, Timeout} |
%%    ignore                             |
%%    {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([Opts]) ->
    State = init_state(Opts),
    case connect(State) of
        State1 when is_record(State1, state) ->
            {ok, State1};
        Err ->
            {stop, Err, State}
    end.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%    {reply, Reply, State, Timeout} |
%%    {noreply, State} |
%%    {noreply, State, Timeout} |
%%    {stop, Reason, Reply, State} |
%%    {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({cmd, Packets}, {From, Ref}, #state{sock=Sock, queue=Queue}=State) ->
    [send(Sock, Packet) || Packet <- Packets],
    Queue1 = lists:foldl(fun(_, Acc) -> queue:in({From, Ref}, Acc) end, Queue, lists:seq(1,length(Packets))),
    {reply, Ref, State#state{queue=Queue1}};

handle_call(_Msg, _From, State) ->
    {reply, unknown_message, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%    {noreply, State, Timeout} |
%%    {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({cancel, Ref}, #state{cancelled=Cancelled}=State) ->
    {noreply, State#state{cancelled=[Ref|Cancelled]}};

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%    {noreply, State, Timeout} |
%%    {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({tcp, Sock, Data}, #state{sock=Sock}=State) ->
    Packet = packet(State, Data),
    case process_packet(State, Packet) of
        {ok, State1} ->
            {noreply, State1};
        Err ->
            {stop, Err, State}
    end;

handle_info({tcp_closed, Sock}, #state{sock=Sock, queue=Queue}=State) ->
    error_logger:error_report(tcp_closed),
    [Pid ! {Ref, closed} || {Pid, Ref} <- queue:to_list(Queue)],
    case connect(State#state{queue = queue:new()}) of
        State1 when is_record(State1, state) ->
            {noreply, State1};
        Err ->
            {stop, Err, State}
    end;

handle_info({tcp_error, Sock, Reason}, #state{sock=Sock}=State) ->
    error_logger:error_report([tcp_error, Reason]),
    {stop, Reason, State};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
init_state(Opts) ->
    Host = proplists:get_value(host, Opts, "localhost"),
    Port = proplists:get_value(port, Opts, 6379),
    Pass = proplists:get_value(pass, Opts),
    #state{
        host = Host,
        port = Port,
        pass = Pass,
        queue = queue:new(),
        cancelled = [],
        buffer = {raw, <<>>}
    }.

connect(#state{host=Host, port=Port, pass=Pass}=State) ->
    SockOpts = [binary, {active, false}, {keepalive, true}, {nodelay, true}],
    case gen_tcp:connect(Host, Port, SockOpts) of
        {ok, Sock} when Pass == undefined; Pass == <<>>; Pass == "" ->
            State#state{sock=Sock};
        {ok, Sock} ->
            case auth(Sock, Pass) of
                ok ->
                    State#state{sock=Sock};
                Err ->
                    Err
            end;
        Err ->
            Err
    end.

auth(Sock, Pass) ->
    case gen_tcp:send(Sock, [<<"AUTH ">>, Pass, <<"\r\n">>]) of
        ok ->
            case gen_tcp:recv(Sock, 0) of
                {ok, <<"+OK\r\n">>} -> ok;
                {ok, Err} -> {error, Err};
                Err -> Err
            end;
        Err ->
            Err
    end.

send(Sock, Packet) ->
    inet:setopts(Sock, [{active, once}]),
    gen_tcp:send(Sock, Packet).

process_packet(#state{sock=Sock, queue=Queue}=State, Packet) ->
    case queue:peek(Queue) of
        {value, {Pid, Ref}} ->
            Fun = fun(Val) -> Pid ! {Ref, Val} end,
            case redo_redis_proto:parse(Fun, Packet) of
                {ok, Rest} ->
                    Pid ! {Ref, done},
                    {_, Queue1} = queue:out(Queue),
                    case Rest of
                        {raw, <<>>} ->
                            {ok, State#state{queue=Queue1, buffer = {raw, <<>>}}};
                        _ ->
                            process_packet(State#state{queue=Queue1}, packet(State#state{buffer=Rest}, <<>>))
                    end;
                {eof, Rest} ->
                    inet:setopts(Sock, [{active, once}]),
                    {ok, State#state{buffer=Rest}}
            end;
        empty ->
            {error, no_destination_for_packet}
    end.

packet(#state{buffer={raw, Buffer}}, Data) ->
    {raw, <<Buffer/binary, Data/binary>>};

packet(#state{buffer={multi_bulk, N, Buffer}}, Data) ->
    {multi_bulk, N, <<Buffer/binary, Data/binary>>}.

