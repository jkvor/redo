%% Manages multiple requests and timers. May or may not kill it if the task
%% takes too long, and then just start a new worker instead.
%%
%% Subscription mode isn't supported yet.
-module(redo_block).
-behaviour(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-export([start_link/0, start_link/1, start_link/2,
         cmd/1, cmd/2, cmd/3, shutdown/1]).
-export([reply/2]).

-record(state, {opts=[], refs, acc,
                worker, worker_status=free,
                timers, queue}).

-define(TIMEOUT, 5000).

%%%%%%%%%%%%%%%%%
%%% INTERFACE %%%
%%%%%%%%%%%%%%%%%
start_link() ->
    start_link([]).

start_link(Name) when is_atom(Name) ->
    start_link(Name, []);
start_link(Opts) when is_list(Opts) ->
    start_link(?MODULE, Opts).

start_link(undefined, Opts) when is_list(Opts) ->
    gen_server:start_link(?MODULE, Opts, []);
start_link(Name, Opts) when is_atom(Name), is_list(Opts) ->
    gen_server:start_link({local, Name}, ?MODULE, Opts, []).


cmd(Cmd) ->
    cmd(?MODULE, Cmd, ?TIMEOUT).

cmd(NameOrPid, Cmd) ->
    cmd(NameOrPid, Cmd, ?TIMEOUT).

cmd(NameOrPid, Cmd, Timeout) ->
    %% format commands to be sent to redis | now done in the worker
    %% Packets = redo_redis_proto:package(Cmd),
    gen_server:call(NameOrPid, {cmd, Cmd, Timeout}, infinity).

shutdown(NameOrPid) ->
    gen_server:cast(NameOrPid, shutdown).

reply({Pid, TimerRef}, Response) ->
    gen_server:cast(Pid, {done, self(), TimerRef, Response}).

%%%%%%%%%%%%%%%%%
%%% CALLBACKS %%%
%%%%%%%%%%%%%%%%%
init(Opts) ->
    process_flag(trap_exit, true), % deal with worker failures
    {ok, Worker} = redo:start_link(undefined, Opts),
    {ok, #state{opts=Opts,
                worker=Worker, worker_status=free,
                timers=gb_trees:empty(), queue=queue:new()}}.

handle_call({cmd, Cmd, TimeOut}, From, S=#state{worker_status=free, timers=T, queue=Q}) ->
    TimerRef = erlang:start_timer(TimeOut, self(), From),
    handle_info(timeout, S#state{timers=gb_trees:insert(TimerRef, {From,Cmd}, T),
                                 queue=queue:in(TimerRef,Q)});
handle_call({cmd, Cmd, TimeOut}, From, S=#state{worker_status=busy, timers=T, queue=Q}) ->
    TimerRef = erlang:start_timer(TimeOut, self(), From),
    {noreply, S#state{timers=gb_trees:insert(TimerRef, {From,Cmd}, T),
                      queue=queue:in(TimerRef,Q)}, 0}.

handle_cast({done, TimerRef, Result}, S=#state{timers=T, queue=Q}) ->
    erlang:cancel_timer(TimerRef),
    case queue:out(Q) of
        {empty, _} -> % response likely came too late
            {noreply, S#state{worker_status=free, refs=undefined, acc=undefined}};
        {{value,TimerRef}, Queue} -> % done, drop top element
            {From, _} = gb_trees:get(TimerRef, T),
            gen_server:reply(From, Result),
            {noreply, S#state{worker_status = free,
                              refs = undefined,
                              acc = undefined,
                              timers = gb_trees:delete(TimerRef, T),
                              queue = Queue}, 0};
        {{value,_Ref}, _} -> % race condition, timed out answer?
            %% Let's clean up just in case
            Queue = queue:filter(fun(Ref) -> Ref =/= TimerRef end, Q),
            {noreply, S#state{worker_status = free,
                              timers = gb_trees:delete_any(TimerRef, T),
                              queue = Queue}, 0}
    end;
handle_cast(shutdown, State) ->
    {stop, {shutdown,cast}, State}.

handle_info(timeout, S=#state{worker_status=busy}) ->
    %% Task scheduled. We'll get a message when it's done
    {noreply, S};
handle_info(timeout, S=#state{worker_status=free}) ->
    case schedule(S) of
        {ok, State} ->  {noreply, State};
        {error, Reason} -> {stop, {worker,Reason}, S}
    end;
handle_info({timeout, TimerRef, From}, S=#state{worker=Pid, timers=T, queue=Q}) ->
    case queue:out(Q) of
        {empty, _} -> % potential bad timer management?
            {noreply, S};
        {{value,TimerRef}, Queue} -> % currently being handled
            unlink(Pid),
            exit(Pid, shutdown),
            {From, _} = gb_trees:get(TimerRef, T),
            gen_server:reply(From, {error, timeout}),
            case schedule(S#state{worker = undefined,
                                  worker_status = free,
                                  refs = undefined,
                                  acc = undefined,
                                  timers = gb_trees:delete(TimerRef, T),
                                  queue = Queue}) of
                {ok, State} -> {noreply, State};
                {error, Reason} -> {stop, {worker,Reason}, S}
            end;
        {_, _} -> % somewhere in the queue, or maybe not if raced before cancel
            case gb_trees:lookup(TimerRef, T) of
                none ->
                    {noreply, S};
                {value, {From, _}} -> % same as the one we got
                    gen_server:reply(From, {error, timeout}),
                    Queue = queue:filter(fun(Ref) -> Ref =/= TimerRef end, Q),
                    {noreply, S#state{timers = gb_trees:delete(TimerRef, T),
                                      queue = Queue}}
            end
    end;
handle_info({'EXIT', Pid, _Reason}, S=#state{worker=Pid}) ->
    %% The worker died. We retry until the timeout fails and the task is pulled
    %% from the queue.
    case schedule(S#state{worker=undefined, worker_status=free,
                          refs=undefined, acc=undefined}) of
        {ok, State} -> {noreply, State};
        {error, Reason} -> {stop, {worker, Reason}, S}
    end;

%% Handling responses from redo
handle_info({Ref, Val}, S=#state{refs=[Ref], acc=Acc, queue=Q}) ->
    {value, TimerRef} = queue:peek(Q),
    Result = case {Val,Acc} of % if Acc [], one single req, drop list.
        {closed, []} -> {error,closed};
        {closed, _} -> lists:reverse([{error,closed} | Acc]);
        {_, []} -> Val;
        _ -> lists:reverse([Val|Acc])
    end,
    gen_server:cast(self(), {done, TimerRef, Result}),
    {noreply, S#state{refs=undefined, acc=undefined}};
handle_info({Ref, closed}, S=#state{refs=[Ref|Refs], acc=Acc}) ->
    {noreply, S#state{refs=Refs, acc=[{error, closed} | Acc]}};
handle_info({Ref, Val}, S=#state{refs=[Ref|Refs], acc=Acc}) ->
    {noreply, S#state{refs=Refs, acc=[Val | Acc]}};
handle_info({_BadRef,_}, State) ->
    {noreply, State}.

terminate(_, #state{worker=undefined}) ->
    ok;
terminate(_, #state{worker=Pid}) ->
    exit(Pid, shutdown),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%%%%%%%%%%%%%
%%% PRIVATE %%%
%%%%%%%%%%%%%%%
schedule(S=#state{worker=undefined, opts=Opts}) ->
    case redo:start_link(undefined, Opts) of
        {ok, Worker} ->
            schedule(S#state{worker=Worker, worker_status=free});
        {stop, Err} ->
            {error, Err}
    end;
schedule(S=#state{worker=Pid, worker_status=free, timers=T, queue=Q}) ->
    case queue:peek(Q) of
        empty -> % nothing to schedule, keep idling.
            {ok, S};
        {value,TimerRef} ->
            {_From, Cmd} = gb_trees:get(TimerRef, T),
            Refs = redo:async_cmd(Pid, Cmd),
            {ok, S#state{worker_status=busy, refs=Refs, acc=[]}}
    end.

