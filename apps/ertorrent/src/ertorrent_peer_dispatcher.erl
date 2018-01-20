-module(ertorrent_peer_dispatcher).
-behaviour(gen_statem).

-export([
         start_link/0,
         stop/1
        ]).

% gen_statem callback functions
-export([
         init/1,
         terminate/3,
         callback_mode/0
        ]).

-record(data, {
               rx_queue::list()
              }).

start_link(ID) ->
    gen_statem:start_link({local, ID}, ?MODULE, [], []).

init([Args]) ->
    State = off,
    Data = 0,
    {ok, State, Data}.

terminate(_Reason, State, Data) ->
    normal.

callback_mode() ->
    state_functions.

% State callback functions

choked(choked, Old_state, Data) ->
    io:format("choked", []),
    {next_state, idle, Data};
choked(Event_type, Event_content, Data) ->
    handle_event(Event_type, Event_content, Data).

unchoked(unchoked, Old_state, Data) ->
    io:format("unchoked", []),
    {next_state, dispatch, Data};
unchoked(Event_type, Event_content, Data) ->
    handle_event(Event_type, Event_content, Data).

feed(feed, Old_state, Data) ->
    io:format("feeding", []),
    ok;
feed(Event_type, Event_content, Data) ->
    handle_event(Event_type, Event_content, Data).

dispatch(dispatch, Old_state, Data) ->
    io:format("dispatching", []),

    case 
    ok;
dispatch(Event_type, Event_content, Data) ->
    handle_event(Event_type, Event_content, Data).

idle(idle, Old_state, Data) ->
    io:format("idling", []),
    {keep_state, Data, };
idle(Event_type, Event_content, Data) ->
    handle_event(Event_type, Event_content, Data).

clear(clear, Old_state, Data) ->
    io:format("clearing", []),
    ok;
clear(Event_type, Event_content, Data) ->
    handle_event(Event_type, Event_content, Data).
