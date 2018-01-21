-module(ertorrent_peer_dispatcher).
-behaviour(gen_statem).

-export([
         choke/1,
         unchoke/1,
         feed/2,
         start_link/1,
         stop/1
        ]).

% gen_statem callback functions
-export([
         init/1,
         terminate/3,
         callback_mode/0
        ]).

% State name callback functions
-export([
         idle/3,
         run/3
        ]).

-record(data, {
               queue::list()
              }).
-type data()::#data{}.

%%% Module API

choke(ID) ->
    gen_statem:cast(ID, choke).

unchoke(ID) ->
    gen_statem:cast(ID, unchoke).

feed(ID, Queue) when is_list(Queue) ->
    gen_statem:cast(ID, {feed, Queue}).

start_link(ID) ->
    gen_statem:start_link({local, ID}, ?MODULE, [], []).

stop(ID) ->
    gen_statem:cast(ID, stop).

dispatch([]) ->
    ok;
dispatch([H| Rest]) ->
    io:format("~p: dispatching '~p'~n", [?FUNCTION_NAME, H]),
    dispatch(Rest).

%%% Behaviour callback functions

init([]) ->
    State = idle,
    Data = #data{queue = []},
    {ok, State, Data}.

terminate(_Reason, _State, _Data) ->
    normal.

callback_mode() ->
    state_functions.

%%% State name callback functions

-spec idle(choke, _Old_state::atom(), Data::data()) -> {keep_state,
                                                        Data::data()} |
                                                       {next_state, run,
                                                        Data::data()}.
idle(cast, choke, Data) ->
    {keep_state, Data};
idle(cast, unchoke, Data) ->
    io:format("idling", []),

    case Data#data.queue == [] of
        false ->
            {next_state, run, Data};
        true ->
            {keep_state, Data}
    end;
idle(Event_type, Event_content, Data) ->
    handle_event(Event_type, Event_content, Data).

run(cast, choke, Data) ->
    io:format("run cast choke~n", []),
    {next_state, idle, Data};
run(cast, unchoke, Data) ->
    io:format("run cast unchoke~n", []),

    case Data#data.queue == [] of
        false ->
            ok = dispatch(Data#data.queue),
            {next_state, idle, #data{queue = []}};
        true ->
            {next_state, idle, Data}
    end;
run(Event_type, Event_content, Data) ->
    handle_event(Event_type, Event_content, Data).

%%% State common events

% @doc Refilling the dispatch queue
% @end
handle_event(cast, {feed, Queue}, Data) ->
    io:format("~p: feed", [?FUNCTION_NAME]),
    New_queue = lists:merge(Queue, Data#data.queue),
    New_data = Data#data{queue = New_queue},
    {keep_state, New_data};

handle_event(cast, stop, Data) ->
    io:format("~p: stop", [?FUNCTION_NAME]),
    {stop, normal, Data};

% Catching everything else
handle_event(_EventType, _EventContent, Data) ->
    {keep_state, Data}.
