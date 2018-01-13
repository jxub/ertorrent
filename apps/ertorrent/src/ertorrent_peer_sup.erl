-module(ertorrent_peer_sup).

-behaviour(supervisor).

-export([
         start_child/8
        ]).

-export([
         start_link/0,
         init/1
        ]).

-define(SETTINGS, ertorrent_settings_srv).

start_child(ID, Block_length, Mode, Info_hash, Peer_id, Piece_length, Socket,
            Torrent_pid) ->
    lager:debug("~p: ~p", [?MODULE, ?FUNCTION_NAME]),
    supervisor:start_child(?MODULE, [ID, Block_length, Mode, Info_hash,
                                     Peer_id, Piece_length, Socket,
                                     Torrent_pid]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Arg) ->
    SupFlags = {simple_one_for_one, 1, 1},

    Peer_worker_specs = {ertorrent_peer_worker,
                         {ertorrent_peer_worker, start_link, []},
                         transient, 60*1000, worker, [ertorrent_peer_worker]},

    {ok, {SupFlags, [Peer_worker_specs]}}.
