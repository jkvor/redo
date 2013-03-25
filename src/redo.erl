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

-include("redo_logging.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/0, start_link/1, start_link/2,
         cmd/1, cmd/2, cmd/3, subscribe/1, subscribe/2,
         shutdown/1, async_cmd/1, async_cmd/2]).

-record(state, {host, port, pass, db, sock, queue, subscriber, cancelled, acc, buffer, reconnect}).

-define(TIMEOUT, 30000).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link([]).

-spec start_link(atom() | list()) ->
                        {ok, pid()} |
                        {error, term()}.
start_link(Name) when is_atom(Name) ->
    start_link(Name, []);

start_link(Opts) when is_list(Opts) ->
    start_link(?MODULE, Opts).

-spec start_link(atom(), list()) -> {ok, pid()} | {error, term()}.
start_link(undefined, Opts) when is_list(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []);

start_link(Name, Opts) when is_atom(Name), is_list(Opts) ->
    gen_server:start_link({local, Name}, ?MODULE, [Opts], []).

cmd(Cmd) ->
    cmd(?MODULE, Cmd, ?TIMEOUT).

cmd(NameOrPid, Cmd) ->
    cmd(NameOrPid, Cmd, ?TIMEOUT).

-type redis_value() :: integer() | binary().
-type response() :: redis_value() | [redis_value()] | {'error', Reason::term()}.

-spec cmd(atom() | pid(), list() | binary(), integer()) ->
                 response() | [response()].

cmd(NameOrPid, Cmd, Timeout) when is_integer(Timeout); Timeout =:= infinity ->
    %% format commands to be sent to redis
    Packets = redo_redis_proto:package(Cmd),

    %% send the commands and receive back
    %% unique refs for each packet sent
    Refs = gen_server:call(NameOrPid, {cmd, Packets}, 2000),
    receive_resps(NameOrPid, Refs, Timeout).

async_cmd(Cmd) ->
    async_cmd(?MODULE, Cmd).

async_cmd(NameOrPid, Cmd) ->
    Packets = redo_redis_proto:package(Cmd),
    gen_server:call(NameOrPid, {cmd, Packets}).

receive_resps(_NameOrPid, {error, Err}, _Timeout) ->
    {error, Err};

receive_resps(NameOrPid, [Ref], Timeout) ->
    %% for a single packet, receive a single reply
    receive_resp(NameOrPid, Ref, Timeout);

receive_resps(NameOrPid, Refs, Timeout) ->
    %% for multiple packets, build a list of replies
    [receive_resp(NameOrPid, Ref, Timeout) || Ref <- Refs].

receive_resp(NameOrPid, Ref, Timeout) ->
    receive
        %% the connection to the redis server was closed
        {Ref, closed} ->
            {error, closed};
        {Ref, Val} ->
            Val
    %% after the timeout expires, cancel the command and return
    after Timeout ->
            gen_server:cast(NameOrPid, {cancel, Ref}),
            {error, timeout}
    end.

-spec subscribe(list() | binary()) -> reference() | {error, term()}.
subscribe(Channel) ->
    subscribe(?MODULE, Channel).

-spec subscribe(atom() | pid(), list() | binary()) -> reference() | {error, term()}.
subscribe(NameOrPid, Channel) ->
    Packet = redo_redis_proto:package(["SUBSCRIBE", Channel]),
    gen_server:call(NameOrPid, {subscribe, Packet}, 2000).

shutdown(NameOrPid) ->
    gen_server:cast(NameOrPid, shutdown).

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
            {stop, Err}
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
handle_call({cmd, Packets}, {From, _Ref}, #state{subscriber=undefined, queue=Queue}=State) ->
    case test_connection(State) of
        State1 when is_record(State1, state) ->
            %% send each packet to redis
            %% and generate a unique ref per packet
            Refs = lists:foldl(
                fun(Packet, Refs) when is_list(Refs) ->
                    case gen_tcp:send(State1#state.sock, Packet) of
                        ok -> [erlang:make_ref()|Refs];
                        Err -> Err
                    end;
                   (_Packet, Err) ->
                    Err
                end, [], Packets),

            %% enqueue the client pid/refs
            case Refs of
                List when is_list(List) ->
                    Refs1 = lists:reverse(Refs),
                    Queue1 = lists:foldl(
                        fun(Ref, Acc) ->
                            queue:in({From, Ref}, Acc)
                        end, Queue, Refs1),
                    {reply, Refs1, State1#state{queue=Queue1}};
                Err ->
                    {stop, Err, State1}
            end;
        _Err ->
            %% failed to connect, retry
            {reply, {error, closed}, State#state{sock=undefined}, 1000}
    end;

handle_call({cmd, _Packets}, _From, State = #state{}) ->
    {reply, {error, subscriber_mode}, State};

handle_call({subscribe, Packet}, {From, _Ref}, State = #state{}) ->
    case test_connection(State) of
        State1 when is_record(State1, state) ->
            case gen_tcp:send(State1#state.sock, Packet) of
                ok ->
                    Ref = erlang:make_ref(),
                    {reply, Ref, State1#state{subscriber={From, Ref}}};
                Err ->
                    {stop, Err, State1}
            end;
        _Err ->
            %% failed to connect, retry
            {reply, {error, closed}, State#state{sock=undefined}, 1000}
    end;

%% state doesn't match. Likely outdated or an error.
%% Moving from 1.1.0 to here sees the addition of one field
handle_call(Msg, From, OldState) when 1+tuple_size(OldState) =:= tuple_size(#state{}),
                                      state =:= element(1,OldState) ->
    {ok, State} = code_change("v1.1.0", OldState, internal_detection),
    handle_call(Msg, From, State);

handle_call(_Msg, _From, State = #state{}) ->
    {reply, unknown_message, State}.


%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%    {noreply, State, Timeout} |
%%    {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(shutdown, State = #state{sock=Sock}) ->
    case is_port(Sock) of
        true ->
            catch gen_tcp:close(Sock);
        false -> ok
    end,
    State1 = close_connection(State#state{sock=undefined}),
    {stop, shutdown, State1};

handle_cast({cancel, Ref}, #state{cancelled=Cancelled}=State) ->
    {noreply, State#state{cancelled=[Ref|Cancelled]}};

%% state doesn't match. Likely outdated or an error.
%% Moving from 1.1.0 to here sees the addition of one field
handle_cast(Msg, OldState) when 1+tuple_size(OldState) =:= tuple_size(#state{}),
                                state =:= element(1,OldState) ->
    {ok, State} = code_change("v1.1.0", OldState, internal_detection),
    handle_cast(Msg, State);

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%    {noreply, State, Timeout} |
%%    {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({tcp, Sock, Data}, #state{sock=Sock, buffer=Buffer}=State) ->
    %% compose the packet to be processed by combining
    %% the leftover buffer with the new data packet
    Packet = packet(Buffer, Data),
    case process_packet(State, Packet) of
        {ok, State1} ->
            %% accept next incoming packet
            inet:setopts(Sock, [{active, once}]),
            {noreply, State1};
        Err ->
            {stop, Err, State}
    end;

handle_info({tcp_closed, Sock}, #state{sock=Sock, reconnect=false}=State) ->
    State1 = close_connection(State),
    {stop, tcp_closed, State1};

handle_info({tcp_closed, Sock}, #state{sock=Sock}=State) ->
    State1 = close_connection(State),

    %% reconnect to redis
    case connect(State1) of
        State2 when is_record(State2, state) ->
            {noreply, State2};
        _Err ->
            {noreply, State1#state{sock=undefined}, 1000}
    end;

handle_info({tcp_error, Sock, Reason}, #state{sock=Sock}=State) ->
    ?WARN("at=tcp_error sock=~p reason=~p",
          [Sock, Reason]),
    CState = close_connection(State#state{sock=undefined}),
    {stop, normal, CState};

%% attempt to reconnect to redis
handle_info(timeout, State = #state{}) ->
    case connect(State) of
        State1 when is_record(State1, state) ->
            {noreply, State1};
        _Err ->
            {noreply, State#state{sock=undefined}, 1000}
    end;

%% state doesn't match. Likely outdated or an error.
%% Moving from 1.1.0 to here sees the addition of one field
handle_info(Msg, OldState) when 1+tuple_size(OldState) =:= tuple_size(#state{}),
                                state =:= element(1,OldState) ->
    {ok, State} = code_change("v1.1.0", OldState, internal_detection),
    handle_info(Msg, State);

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
code_change("v1.1.0", OldState, internal_detection) ->
    {state, Host, Port, Pass, Db, Sock, Queue, Subscriber, Cancelled,
            Acc, Buffer} = OldState,
    %% we make the reconnection set to 'true' given that's the default
    %% value set in init_state/1
    {ok, #state{host=Host, port=Port, pass=Pass, db=Db, sock=Sock,
                queue=Queue, subscriber=Subscriber, cancelled=Cancelled,
                acc=Acc, buffer=Buffer, reconnect=true}};
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
init_state(Opts) ->
    Host = proplists:get_value(host, Opts, "localhost"),
    Port = proplists:get_value(port, Opts, 6379),
    Pass = proplists:get_value(pass, Opts),
    Db   = proplists:get_value(db, Opts, 0),
    Recn = proplists:get_value(reconnect, Opts, true),
    #state{
        host = Host,
        port = Port,
        pass = Pass,
        db = Db,
        queue = queue:new(),
        cancelled = [],
        acc = [],
        buffer = {raw, <<>>},
        reconnect = Recn
    }.

connect(#state{host=Host, port=Port, pass=Pass, db=Db}=State) ->
    case connect_socket(Host, Port) of
        {ok, Sock} ->
            case auth(Sock, Pass) of
                ok ->
                    case select_db(Sock, Db) of
                        ok ->
                            inet:setopts(Sock, [{active, once}]),
                            State#state{sock=Sock};
                        Err ->
                            Err
                    end;
                Err ->
                    Err
            end;
        Err ->
            Err
    end.

connect_socket(Host, Port) ->
    SockOpts = [binary, {active, false}, {keepalive, true}, {nodelay, true}],
    gen_tcp:connect(Host, Port, SockOpts).

auth(_Sock, Pass) when Pass == <<>>; Pass == undefined ->
    ok;

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

select_db(_Sock, 0) ->
    ok;

select_db(Sock, Db) ->
    case gen_tcp:send(Sock, hd(redo_redis_proto:package(["SELECT", Db]))) of
        ok ->
            case gen_tcp:recv(Sock, 0) of
                {ok, <<"+OK\r\n">>} -> ok;
                {ok, Err} -> {error, Err};
                Err -> Err
            end;
        Err ->
            Err
    end.

test_connection(#state{sock=undefined}=State) ->
    connect(State);

test_connection(State) ->
    State.

process_packet(#state{acc=Acc, queue=Queue, subscriber=Subscriber}=State, Packet) ->
    case redo_redis_proto:parse(Acc, Packet) of
        %% the reply has been received in its entirety
        {ok, Result, Rest} ->
            case queue:out(Queue) of
                {{value, {Pid, Ref}}, Queue1} ->
                    send_response(Pid, Ref, Result, Rest, State, Queue1);
                {empty, Queue1} ->
                    case Subscriber of
                        {Pid, Ref} ->
                            send_response(Pid, Ref, Result, Rest, State, Queue1);
                        undefined ->
                            {error, queue_empty}
                    end
            end;
        %% the current reply packet ended abruptly
        %% we must wait for the next data packet
        {eof, Acc1, Rest} ->
            {ok, State#state{acc=Acc1, buffer=Rest}}
    end.

send_response(Pid, Ref, Result, Rest, State, Queue) ->
    Pid ! {Ref, Result},
    case Rest of
        {raw, <<>>} ->
            %% we have reached the end of this tcp packet
            %% wait for the next incoming packet
            {ok, State#state{acc=[], queue=Queue, buffer = {raw, <<>>}}};
        _ ->
            %% there is still data left in this packet
            %% we may begin processing the next reply
            process_packet(State#state{acc=[], queue=Queue}, packet(Rest, <<>>))
    end.

packet({raw, Buffer}, Data) ->
    {raw, <<Buffer/binary, Data/binary>>};

packet({multi_bulk, N, Buffer}, Data) ->
    {multi_bulk, N, <<Buffer/binary, Data/binary>>}.

close_connection(State = #state{queue=Queue}) ->
    %% notify all waiting pids that the connection is closed
    %% so that they may try resending their requests
    [Pid ! {Ref, closed} || {Pid, Ref} <- queue:to_list(Queue)],

    %% notify subscriber pid of disconnect
    case State#state.subscriber of
        {Pid, Ref} -> Pid ! {Ref, closed};
        _ -> ok
    end,

    %% reset the state
    State#state{
        queue = queue:new(),
        cancelled = [],
        buffer = {raw, <<>>}
    }.
