-module(ertorrent_peer_statem_sup).

-behaviour(supervisor).

-export([
         start_child/8
        ]).

-export([
         start_link/0,
         init/1
        ]).

start_child(ID) ->
    lager:debug("~p: ~p", [?MODULE, ?FUNCTION_NAME]),
    supervisor:start_child(?MODULE, [ID]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Arg) ->
    SupFlags = {simple_one_for_one, 1, 1},

    Peer_statem_specs = {ertorrent_peer_statem,
                         {ertorrent_peer_statem, start_link, []},
                         transient, 60*1000, worker, [ertorrent_peer_statem]},

    {ok, {SupFlags, [Peer_statem_specs]}}.
