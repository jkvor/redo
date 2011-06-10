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
         cmd/1, cmd/2, cmd/3, subscribe/1, subscribe/2]).

-record(state, {host, port, pass, db, sock, queue, subscriber, cancelled, acc, buffer}).

-define(TIMEOUT, 30000).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link([]).

-spec start_link(atom()) -> {ok, pid()} | {error, term()}.
start_link(Name) when is_atom(Name) ->
    start_link(Name, []);

start_link(Opts) when is_list(Opts) ->
    start_link(?MODULE, Opts).

-spec start_link(atom(), list()) -> {ok, pid()} | {error, term()}.
start_link(undefined, Opts) when is_list(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []);

start_link(Name, Opts) when is_atom(Name), is_list(Opts) ->
    gen_server:start_link({local, Name}, ?MODULE, [Opts], []).

-spec cmd(list() | binary()) -> list() | binary() | integer().
cmd(Cmd) ->
    cmd(?MODULE, Cmd, ?TIMEOUT).

-spec cmd(atom() | pid(), list() | binary()) -> list() | binary() | integer().
cmd(NameOrPid, Cmd) ->
    cmd(NameOrPid, Cmd, ?TIMEOUT).

-spec cmd(atom() | pid(), list() | binary(), integer()) -> list() | binary() | integer().
cmd(NameOrPid, Cmd, Timeout) when is_integer(Timeout) ->
    %% format commands to be sent to redis
    Packets = redo_redis_proto:package(Cmd),

    %% send the commands and receive back
    %% unique refs for each packet sent
    Refs = gen_server:call(NameOrPid, {cmd, Packets}, 2000),
    receive_resps(NameOrPid, Refs, Timeout).

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
        Err ->
            error_logger:error_report({connect, Err}),
            %% failed to connect, retry
            {reply, {error, closed}, State#state{sock=undefined}, 1000}
    end;

handle_call({cmd, _Packets}, _From, State) ->
    {reply, {error, subscriber_mode}, State};

handle_call({subscribe, Packet}, {From, _Ref}, State) ->
    case test_connection(State) of
        State1 when is_record(State1, state) ->
            case gen_tcp:send(State1#state.sock, Packet) of
                ok ->
                    Ref = erlang:make_ref(),
                    {reply, Ref, State1#state{subscriber={From, Ref}}};
                Err ->
                    {stop, Err, State1}
            end;
        Err ->
            error_logger:error_report({connect, Err}),
            %% failed to connect, retry
            {reply, {error, closed}, State#state{sock=undefined}, 1000}
    end;

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

handle_info({tcp_closed, Sock}, #state{sock=Sock, queue=Queue}=State) ->
    error_logger:error_report(tcp_closed),

    %% notify all waiting pids that the connection is closed
    %% so that they may try resending their requests
    [Pid ! {Ref, closed} || {Pid, Ref} <- queue:to_list(Queue)],

    %% notify subscriber pid of disconnect
    case State#state.subscriber of
        {Pid, Ref} -> Pid ! {Ref, closed};
        _ -> ok
    end,

    %% reset the state
    State1 = State#state{
        queue = queue:new(),
        cancelled = [],
        buffer = {raw, <<>>}
    },

    %% reconnect to redis
    case connect(State1) of
        State2 when is_record(State2, state) ->
            {noreply, State2};
        Err ->
            error_logger:error_report({connect, Err}),
            {noreply, State1#state{sock=undefined}, 1000}
    end;

handle_info({tcp_error, Sock, Reason}, #state{sock=Sock}=State) ->
    error_logger:error_report([tcp_error, Reason]),
    {stop, Reason, State};

%% attempt to reconnect to redis
handle_info(timeout, State) ->
    case connect(State) of
        State1 when is_record(State1, state) ->
            {noreply, State1};
        Err ->
            error_logger:error_report({connect, Err}),
            {noreply, State#state{sock=undefined}, 1000}
    end;

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
    Db   = proplists:get_value(db, Opts, 0),
    #state{
        host = Host,
        port = Port,
        pass = Pass,
        db = Db,
        queue = queue:new(),
        cancelled = [],
        acc = [],
        buffer = {raw, <<>>}
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

