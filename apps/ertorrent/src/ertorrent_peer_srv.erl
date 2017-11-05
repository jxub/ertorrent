%%%-------------------------------------------------------------------
%%% @todo read up on edoc
%%% @doc peer_srv.
%%% @end
%%%-------------------------------------------------------------------

-module(ertorrent_peer_srv).

-behaviour(gen_server).

-export([
         add_rx_peer/2,
         add_rx_peers/2,
         add_tx_peer/2,
         multicast/2,
         remove/1,
         remove_related/1
        ]).

-export([start_link/0,
         stop/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("ertorrent_log.hrl").

-define(SETTINGS_SRV, ertorrent_settings_srv).
-define(PEER_SUP, ertorrent_peer_sup).
-define(PEER_SSUP, ertorrent_peer_ssup).
-define(PEER_W, ertorrent_peer_worker).
-define(TORRENT_SRV, ertorrent_torrent_srv).
-define(TORRENT_W, ertorrent_torrent_worker).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {peers=[],
                own_peer_id,
                port_max,
                port_min}).

%%% Client API %%%
add_rx_peer(Info_hash, {Address, Port}) ->
    gen_server:cast(?MODULE, {peer_s_add_rx_peer, self(), {Info_hash, {Address, Port}}}).

-spec add_rx_peers(Info_hash::string(), Peers::[tuple()]) -> ok.
add_rx_peers(Info_hash, Peers) when is_list(Peers) ->
    gen_server:cast(?MODULE, {peer_s_add_rx_peers, self(), {Info_hash, Peers}}).

add_tx_peer(Info_hash, Socket) ->
    gen_server:cast(?MODULE, {peer_s_add_tx_peer, self(), {Socket, Info_hash}}).

multicast(Info_hash, Message) ->
    gen_server:cast(?MODULE, {multicast, {Info_hash, Message}}).

remove(Address) ->
    gen_server:call(?MODULE, {remove, Address}).

remove_related(Info_hash) ->
    gen_server:call(?MODULE, {remove_related, Info_hash}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    io:format("Stopping: ~p...~n", [?MODULE]),
    gen_server:cast(?MODULE, stop).

%%% Internal functions
start_rx_peer(From, Info_hash, {Address, Port}, Own_peer_id) when is_binary(Info_hash) ->
    ID = erlang:unique_integer(),
    Ret = ?PEER_SUP:start_child(ID, Info_hash, Own_peer_id, {Address, Port}, From),

    case Ret of
        % TODO Atm the handling of the {ok, _} responses is redundant.
        % It's unlikely that both will be used however in the writing
        % moment this cannot be determined and therefore both are taken
        % into consideration until one could be ruled out.
        {ok, Peer_pid} ->
            lager:debug("~p: ~p: connecting to peer '~p:~p'", [?MODULE,
                                                               ?FUNCTION_NAME,
                                                               Address,
                                                               Port]),
            ok = ?PEER_W:connect(Peer_pid),

            {ID, {Address, Port}, Info_hash};
        {ok, Peer_pid, Info} ->
            lager:warning("recv unhandled data Info: '~p'", [Info]),
            ok = ?PEER_W:activate(Peer_pid),

            {ID, {Address, Port}, Info_hash};
        % This is rather unexpected so log it until it is clear when it happens
        {error, Reason_sup} ->
            lager:error("peer_srv failed to spawn a peer_worker (rx), check the peer_sup. reason: '~p'",
                        [Reason_sup]),

            % TODO if there's an issue with the peer_sup being
            % unresponsive. An alternative could be to message the
            % peer_ssup to restart the sup. For now it will only be
            % logged.

            error
    end.

%%% Callback module
init(_Args) ->
    {peer_id_str, Own_peer_id} = ?SETTINGS_SRV:get_sync(peer_id_str),
    {ok, #state{own_peer_id = Own_peer_id}, hibernate}.

handle_call(Req, From, State) ->
    ?INFO("unhandled call request: " ++ Req ++ ", from: " ++ From),
    {noreply, State}.

handle_cast({peer_s_add_rx_peer, From, {Info_hash, {Address, Port}}}, State) ->
    lager:debug("~p: ~p: peer_s_add_rx_peer", [?MODULE, ?FUNCTION_NAME]),

    Ref = start_rx_peer(From, Info_hash, {Address, Port}, State#state.own_peer_id),

    From ! {peer_s_rx_peer, Ref},

    {noreply, State, hibernate};

handle_cast({peer_s_add_rx_peers, From, {Info_hash, Peers}}, State) ->
    lager:debug("~p: ~p: peer_s_add_rx_peers", [?MODULE, ?FUNCTION_NAME]),

    % Setup peer connections, succeeding peers will return a reference and
    % failing 'error'.
    Fold_results = fun(Peer, Acc) ->
                       Ref = start_rx_peer(From, Info_hash, Peer,
                                           State#state.own_peer_id),

                       [Ref| Acc]
                   end,
    Peer_results = lists:foldl(Fold_results, [], Peers),

    % Create a list of connected peer workers
    Filter_errors = fun(Res) ->
                        case Res of
                            error -> false;
                            _ -> true
                        end
                    end,
    Succeeded_peers = lists:filter(Filter_errors, Peer_results),

    % Count the amount of failed peer workers
    Nbr_of_failed_peers = length(Peers) - length(Succeeded_peers),

    case Nbr_of_failed_peers > 0 of
        true ->
            % Request new peers corresponding to the amount of the failed peers
            ok = ?TORRENT_W:request_peers(From, Nbr_of_failed_peers);
        false ->
            lager:debug("~p: ~p: reached expected amount of running peers",
                        [?MODULE, ?FUNCTION_NAME])
    end,

    % Send back the list of connected peers
    From ! {peer_s_rx_peers, Succeeded_peers},

    {noreply, State, hibernate};

% @doc Adding a transmitting peer worker. The socket is already accepted by the
% peer accept-process. Note: When mentioning the terms receiving and
% transmitting, about peers, it refers to the function of the peer worker
% process and not the actual peer on the other side of the wire.
% @end
handle_cast({add_tx_peer, {Socket, Info_hash}}, State) ->
    ID = erlang:unique_integer(),

    % Retreive the address and port from the socket
    case inet:peername(Socket) of
        {ok, {S_address, S_port}} ->
            Address = S_address,
            Port = S_port;
        {error, Reason_peername} ->
            ?WARNING("failed to retreive address and port form peer_tx socket: "
                     ++ Reason_peername),

            % Set fail-over values for Address and Port, these values are
            % mainly for information so should not be vital.
            Address = unknown,
            Port = unknown
    end,

    Peers = State#state.peers,

    % Validate the Info_hash
    case ?TORRENT_SRV:member_by_info_hash(Info_hash) of
        true ->
            case supervisor:start_child(?PEER_SUP,
                                         [ID,
                                          [ID,
                                           Info_hash,
                                           State#state.own_peer_id,
                                           Socket]]) of
                {ok, Peer_pid} ->
                    New_state = State#state{peers=[{ID,
                                                    Peer_pid,
                                                    Address,
                                                    Port,
                                                    Info_hash}| Peers]};
                {ok, Peer_pid, _Info} ->
                    New_state = State#state{peers=[{ID,
                                                    Peer_pid,
                                                    Address,
                                                    Port,
                                                    Info_hash}| Peers]};
                {error, Reason_sup} ->
                    ?ERROR("peer_srv failed to spawn a peer_worker (rx), check the peer_sup. reason: "
                           ++ Reason_sup),

                    % TODO if there's an issue with the peer_sup being
                    % unresponsive. An alternative could be to message the
                    % peer_ssup to restart the sup. For now it will only be
                    % logged.

                    New_state = State
            end;
        false ->
            ?WARNING("incoming peer request for a non-existing torrent with hash:"
                     ++ Info_hash),

            New_state = State
    end,
    {noreply, New_state, hibernate};
handle_cast({multicast, {Info_hash_x, Message}}, State) ->
    F = fun({ID, _Address, _Port, Info_hash_y}) ->
        case Info_hash_x =:= Info_hash_y of
            true ->
                ID ! {peer_s_multicast, Message}
        end
    end,

    lists:foreach(F, State#state.peers),

    {noreply, State, hibernate};
handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info({'EXIT', _ParentPid, shutdown}, State) ->
    {stop, shutdown, State};
handle_info(_Info, _State) ->
    ok.

terminate(Reason, State) ->
    io:format("~p: going down, Reason: ~p~n", [?MODULE, Reason]),
    error_logger:info_msg("~p: terminating, reason: ~p~n", [?MODULE, Reason]),
    {ok, State}.

code_change(_OldVsn, _State, _Extra) ->
    {ok}.
