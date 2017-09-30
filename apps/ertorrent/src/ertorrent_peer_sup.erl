-module(ertorrent_peer_sup).

-behaviour(supervisor).

-export([
         start_child/4
        ]).

-export([
         start_link/0,
         init/1
        ]).

-define(SETTINGS, ertorrent_settings_srv).

start_child(ID, Info_hash, Own_peer_id, Socket) ->
    supervisor:start_child(?MODULE, [ID, [ID, Info_hash, Peer_id_own,
                                          Socket]]),

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Arg) ->
    SupFlags = {simple_one_for_one, 4, 5},

    Peer_worker_specs = {ertorrent_peer_worker,
                         {ertorrent_peer_worker, start_link, []},
                         transient, 60*1000, worker, [ertorrent_peer_worker]},

    {ok, {SupFlags, [Peer_worker_specs]}}.
