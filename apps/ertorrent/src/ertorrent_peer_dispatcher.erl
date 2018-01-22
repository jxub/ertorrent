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
-type event_content()::choke | unchoke | stop | {feed, Queue::list()}.
-type idle_result()::{keep_state, Data::data()} |
        {next_state, run, Data::data()} |
        {next_state, run, Data::data(), [{next_event, cast, unchoke}]} |
        {repeat_state, Data::data()}.
-type run_result()::{keep_state, Data::data()} |
        {next_state, idle, Data::data()} |
        {next_state, idle, Data::data(), [{next_event, cast, unchoke}]}.

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

-spec init(Args::list()) -> {ok, idle, data()}.
init(_Args) ->
    State = idle,
    Data = #data{queue = []},
    {ok, State, Data}.

terminate(_Reason, _State, _Data) ->
    % TODO return the remaining queue, if there is one, when terminating
    normal.

callback_mode() ->
    state_functions.

%%% State name callback functions

-spec idle(cast, Old_state::event_content(), Data::data()) -> idle_result().
idle(cast, choke, Data) ->
    lager:debug("~p: ~p: choke", [?MODULE, ?FUNCTION_NAME]),
    {keep_state, Data};
idle(cast, unchoke, Data) ->
    lager:debug("~p: ~p: unchoke", [?MODULE, ?FUNCTION_NAME]),
    case Data#data.queue == [] of
        false ->
            {next_state, run, Data, [{next_event, cast, unchoke}]};
        true ->
            {keep_state, Data}
    end;
idle(cast, {feed, Queue}, Data) ->
    lager:debug("~p: ~p: feed '~p'", [?MODULE, ?FUNCTION_NAME, Queue]),
    New_queue = lists:merge(Queue, Data#data.queue),
    % Refilling the dispatch queue and continue to idle.
    New_data = Data#data{queue = New_queue},
    {repeat_state, New_data};
idle(Event_type, Event_content, Data) ->
    handle_event(Event_type, Event_content, Data).

-spec run(cast, Old_state::event_content(), Data::data()) -> run_result().
run(cast, choke, Data) ->
    lager:debug("~p: ~p: choke", [?MODULE, ?FUNCTION_NAME]),
    {next_state, idle, Data};
run(cast, unchoke, Data) ->
    lager:debug("~p: ~p: unchoke", [?MODULE, ?FUNCTION_NAME]),

    case Data#data.queue == [] of
        false ->
            ok = dispatch(Data#data.queue),
            {keep_state, #data{queue = []}};
        true ->
            {keep_state, Data}
    end;
run(cast, {feed, Queue}, Data) ->
    lager:debug("~p: ~p: feed '~p'", [?MODULE, ?FUNCTION_NAME, Queue]),

    New_queue = lists:merge(Queue, Data#data.queue),
    New_data = Data#data{queue = New_queue},

    % Since current state is 'run' it is safe to assume that the previous event
    % is unchoke. By changing state to idle and emit another event another
    % dispatch should be triggered.
    {next_state, idle, New_data, [{next_event, cast, unchoke}]};
run(Event_type, Event_content, Data) ->
    handle_event(Event_type, Event_content, Data).

%%% State common events

handle_event(cast, stop, Data) ->
    lager:debug("~p: ~p: stop", [?MODULE, ?FUNCTION_NAME]),
    {stop, normal, Data};

% Catching everything else
handle_event(_EventType, _EventContent, Data) ->
    {keep_state, Data}.
