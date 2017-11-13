%%%-------------------------------------------------------------------
%%% @todo read up on edoc
%%% @doc torrent_gen, this is the interface to interact with the
%%% torrents.
%%% @end
%%% TODO:
%%%-------------------------------------------------------------------

-module(ertorrent_peer_worker).

-behaviour(gen_server).

-export([
         connect/1,
         reset_rx_keep_alive/1,
         reset_tx_keep_alive/1,
         send_keep_alive/1
        ]).

-export([
         start_link/5,
         stop/1
        ]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-include("ertorrent_log.hrl").
-include("ertorrent_peer_tcp_message_ids.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {
                address,
                request_buffer::list(),
                dht_port,
                id, % peer_worker_id
                incoming_piece::binary(),
                incoming_piece_blocks::list(),
                incoming_piece_hash,
                incoming_piece_index,
                incoming_piece_downloaded_size,
                incoming_piece_total_size,
                % Piece queue is a list containing tuples with information for each piece
                % {index, hash, piece_size}
                incoming_piece_queue::list(),
                % Timer reference for when keep alive is expected from peer
                keep_alive_rx_ref,
                % Timer reference for when keep alive message should be sent
                keep_alive_tx_ref,
                % Piece queue is a list containing tuples with information for each piece
                % {index, hash, data}
                % outgoing_piece_queue will hold all the requested pieces until
                % they're completely transfered
                outgoing_piece_queue::list(),
                peer_bitfield,
                peer_id,
                peer_srv_pid,
                peer_choked,
                peer_interested,
                port::integer(),
                received_handshake::boolean(),
                socket,
                self_choked,
                self_interested,
                state_incoming,
                state_outgoing,
                torrent_bitfield,
                torrent_info_hash,
                torrent_info_hash_bin,
                torrent_peer_id,
                torrent_pid
               }).

% 16KB seems to be an inofficial standard among most torrent client
% implementations. Link to the discussion:
% https://wiki.theory.org/Talk:BitTorrentSpecification#Messages:_request
-define(BLOCK_SIZE, 16000).
% Can't make the same assumption for external peers so this should be two
% minutes.
-define(KEEP_ALIVE_RX_TIMER, 120000).
% Should send keep-alive within two minutes. A little less than two minutes
% should be fine.
-define(KEEP_ALIVE_TX_TIMER, 100000).

-define(PEER_SRV, ertorrent_peer_srv).
-define(BINARY, ertorrent_binary_utils).
-define(PEER_PROTOCOL, ertorrent_peer_tcp_protocol).

%%% Extended client API

% Instructing the peer worker to start leeching
connect(ID) ->
    gen_server:cast(ID, peer_w_connect).

%%% Standard client API
start_link(ID, Info_hash, Peer_id, Socket, Torrent_pid) ->
    lager:debug("~p: ~p", [?MODULE, ?FUNCTION_NAME]),
    gen_server:start_link(?MODULE, [ID, Info_hash, Peer_id, Socket, Torrent_pid], [{hibernate_after, 2000}]).

stop(ID) ->
    io:format("stopping~n"),
    gen_server:cast({?MODULE, ID}, stop).

%%% Internal functions
reset_rx_keep_alive(Keep_alive_rx_ref) ->
    erlang:cancel(Keep_alive_rx_ref),

    erlang:send_after(?KEEP_ALIVE_RX_TIMER, self(), {keep_alive_rx_timeout}).

reset_tx_keep_alive(Keep_alive_tx_ref) ->
    erlang:cancel(Keep_alive_tx_ref),

    erlang:send_after(?KEEP_ALIVE_TX_TIMER, self(), {keep_alive_tx_timeout}).

send_keep_alive(Socket) ->
    TimerRef = erlang:send_after(?KEEP_ALIVE_TX_TIMER, self(), {keep_alive_internal_timeout}),

    case gen_tcp:send(Socket, <<0:32>>) of
        ok ->
            {ok, TimerRef};
        {error, Reason} ->
            erlang:cancel(TimerRef),
            {error, Reason}
    end.

blocks_to_piece(Blocks) ->
    Sorted_blocks = lists:keysort(1, Blocks),

    lager:debug("TODO This might work ..."),
    Blocks_tmp = lists:foldl(fun({_Idx, Data}, Total) ->
                                [Data| Total]
                             end, [], Sorted_blocks),

    Blocks_list = lists:reverse(Blocks_tmp),

    lager:debug("Converting list to binary"),
    list_to_binary(Blocks_list).

% Call peer_srv to forward a finished piece to the file_srv and prepare a state
% for the next piece.
complete_piece(State) ->
    Blocks = State#state.incoming_piece_blocks,
    Piece = blocks_to_piece(Blocks),

    % Announce the finished piece to the other subsystems via the peer_srv
    State#state.peer_srv_pid ! {peer_w_piece_ready,
                                State#state.torrent_pid,
                                State#state.id,
                                State#state.incoming_piece_index,
                                Piece},

    [{New_piece_index,
      New_piece_hash,
      New_piece_total_size}| New_piece_queue] = State#state.incoming_piece_queue,

    New_state = State#state{incoming_piece_blocks = [],
                            incoming_piece_downloaded_size = 0,
                            incoming_piece_index = New_piece_index,
                            incoming_piece_queue = New_piece_queue,
                            incoming_piece_total_size = New_piece_total_size,
                            incoming_piece_hash = New_piece_hash},

    {ok, New_state}.

handle_bitfield(Bitfield, State) ->
    {ok, Bf_list} = ?BINARY:bitfield_to_list(Bitfield),
    lager:info("~p: ~p: peer '~p' received bitfield: '~p', list: '~p'",
               [?MODULE, ?FUNCTION_NAME, State#state.id, Bitfield, Bf_list]),

    State#state.torrent_pid ! {peer_w_bitfield, State#state.id, Bitfield},

    {ok, State}.

handle_have(Piece_idx, State) ->
    lager:debug("~p: HAVE, piece index: '~p'", [?FUNCTION_NAME, Piece_idx]),

    New_bitfield = ?BINARY:set_bit(Piece_idx, 1, State#state.peer_bitfield),

    State#state.peer_srv_pid ! {peer_w_bitfield_update, New_bitfield},
    % TODO if we're sending a piece that is announced in a HAVE message, should
    % we cancel the tranmission or ignore it and wait for a CANCEL message?

    % Removing the cached piece
    Outgoing_pieces = lists:keydelete(Piece_idx, 1, State#state.outgoing_piece_queue),

    New_state = State#state{outgoing_piece_queue = Outgoing_pieces,
                            peer_bitfield = New_bitfield},

    {ok, New_state}.
handle_request(Index, Begin, Length, State) ->
    lager:debug("~p: REQUEST, index: '~p', begin: '~p', length: '~p'",
                [?FUNCTION_NAME, Index, Begin, Length]),

    case lists:keyfind(Index, 1, State#state.outgoing_piece_queue) of
        {Index, _Hash, Data} ->
            <<Begin, Block:Length, _Rest/binary>> = Data,

            Msg = ?PEER_PROTOCOL:msg_piece(Length, Index, Begin, Block),

            % TODO figure out a smart way to detect when we can clear out
            % cached pieces
            case gen_tcp:send(State#state.socket, Msg) of
                {error, Reason} ->
                    lager:warning("~p: ~p: failed to send a REQUEST response: '~p'",
                                  [?MODULE, ?FUNCTION_NAME, Reason])
            end,

            New_state = State;
        false ->
            % If the requested piece ain't buffered, send a request to read the
            % piece form disk and meanwhile buffer the request.
            Request_buffer = [{request, Index, Begin, Length}|
                              State#state.request_buffer],

            State#state.peer_srv_pid ! {peer_w_piece_req, self(), Index},

            New_state = State#state{request_buffer=Request_buffer}
    end,

    {ok, New_state}.
handle_piece(Index, Begin, Data, State) ->
    % Check that the piece is requested
    case Index == State#state.incoming_piece_index of
        true ->
            Size = State#state.incoming_piece_downloaded_size + binary:referenced_byte_size(Data),

            case Size of
                % If the size is smaller then the total expected amount, keep building the piece
                Size when Size < State#state.incoming_piece_total_size ->
                    New_blocks = [{Begin, Data}| State#state.incoming_piece_blocks],
                    New_size = Size,

                    New_state = State#state{incoming_piece_blocks = New_blocks,
                                            incoming_piece_downloaded_size = New_size};
                % If it's the expected size, complete the piece
                Size when Size == State#state.incoming_piece_total_size ->
                    {ok, New_state} = complete_piece(State);
                % If the size exceeds the expected size, something has gone terribly wrong
                Size when Size > State#state.incoming_piece_total_size ->
                    ?ERROR("Size of the piece is larger than it is supposed to. Discarding blocks"),
                    New_state = State
            end;
        false ->
            ?WARNING("Received blocks belonging to another piece"),
            New_state = State
    end,
    {ok, New_state}.

% TODO determine if we should clear the request buffer when receiving choked?
% According to http://jonas.nitro.dk/bittorrent/bittorrent-rfc.txt
% "If a peer chokes a remote peer, it MUST also discard any unanswered
% requests for blocks previously received from the remote peer."
handle_choke(State) ->
    % Inform the other subsystems that this peer is choked
    % TODO I am adding this for the future if we want to display this in any way
    State#state.peer_srv_pid ! {peer_w_choke, State#state.id},

    % Updating the peer state and clearing the request buffer
    New_state = State#state{peer_choked = true,
                            request_buffer = []},

    {ok, New_state}.
handle_unchoke(State) ->
    State#state.peer_srv_pid ! {peer_w_unchoke, State#state.id},

    % Updating the peer state
    New_state = State#state{peer_choked = false},

    {ok, New_state}.
handle_interested(State) ->
    State#state.peer_srv_pid ! {peer_w_interested, State#state.id},

    % Updating the peer state
    New_state = State#state{peer_interested = true},

    {ok, New_state}.
% TODO figure out how to handle not interested
handle_not_interested(State) ->
    State#state.peer_srv_pid ! {peer_w_not_interested, State#state.id},

    % Updating the peer state
    New_state = State#state{peer_interested = false},

    {ok, New_state}.
handle_cancel(Index, Begin, Length, State) ->
    % Remove the buffered request and possible duplicates
    New_buffered_requests = lists:filter(
                                fun(X) ->
                                    case X of
                                        {request, Index, Begin, Length} -> false;
                                        _ -> true
                                    end
                                end,
                                State#state.request_buffer
                            ),

    % Check if there's any remaining requests for the same piece, otherwise
    % remove the piece from the cache.
    case lists:keyfind(Index, 2, New_buffered_requests) of
        false ->
            New_tx_pieces = lists:keydelete(Index, 1,
                                            State#state.outgoing_piece_queue);
        _ -> New_tx_pieces = State#state.outgoing_piece_queue
    end,

    New_state = State#state{request_buffer = New_buffered_requests,
                            outgoing_piece_queue = New_tx_pieces},

    {ok, New_state}.
handle_port(Port, State) ->
    New_state = State#state{dht_port = Port},
    {ok, New_state}.

parse_peer_flags(Flags_bin) ->
    lager:debug("~p: ~p: parse_peer_flags", [?MODULE, ?FUNCTION_NAME]),

    Fast_extension = 16#4 band Flags_bin == 16#04,

    Flags = [{fast_extension, Fast_extension}],
    {ok, Flags}.

%%% Callback module
init([ID, Info_hash, Peer_id, {Address, Port}, Torrent_pid]) when is_integer(ID)
                                                             andalso is_binary(Info_hash) ->
    lager:debug("~p: ~p", [?MODULE, ?FUNCTION_NAME]),
    {ok, #state{id=ID,
                address=Address,
                peer_id=Peer_id, % TODO Should this be retreived from settings_srv?
                peer_choked = true,
                peer_interested = true,
                port=Port,
                received_handshake = false,
                torrent_info_hash_bin=Info_hash,
                torrent_pid=Torrent_pid}, hibernate}.

terminate(Reason, State) ->
    lager:debug("~p: ~p: terminating, reason: '~p'", [?MODULE, ?FUNCTION_NAME, Reason]),

    case State#state.keep_alive_rx_ref /= undefined of
        false ->
            ok;
        true ->
            erlang:cancel_timer(State#state.keep_alive_rx_ref)
    end,

    case State#state.keep_alive_tx_ref /= undefined of
        false ->
            ok;
        true ->
            erlang:cancel_timer(State#state.keep_alive_tx_ref)
    end,

    case State#state.socket /= undefined of
        false ->
            ok;
        true ->
            ok = gen_tcp:close(State#state.socket)
    end,

    State#state.torrent_pid ! {peer_w_terminate,
                               State#state.id,
                               State#state.incoming_piece_index},

    ok.

%% Synchronous
handle_call(_Req, _From, State) ->
    {noreply, State}.

%% Asynchronous
handle_cast(peer_w_connect, State) ->
    lager:debug("~p: ~p: connect", [?MODULE,
                                    ?FUNCTION_NAME]),

    Peer_id = list_to_binary(State#state.peer_id),
    {ok, Handshake} = ?PEER_PROTOCOL:msg_handshake(State#state.torrent_info_hash_bin,
                                                   Peer_id),

    case gen_tcp:connect(State#state.address,
                         State#state.port, [binary, {packet, 0}], 2000) of
        {ok, Socket} ->
            lager:debug("~p: ~p: connected to peer '~p:~p'", [?MODULE,
                                                              ?FUNCTION_NAME,
                                                              State#state.address,
                                                              State#state.port
                                                             ]),
            case gen_tcp:send(Socket,
                              Handshake) of
                ok ->
                    lager:debug("~p: ~p: sent handshake", [?MODULE, ?FUNCTION_NAME]),
                    % This is automatically canceled if the process terminates
                    Keep_alive_tx_ref = erlang:send_after(?KEEP_ALIVE_TX_TIMER,
                                                          self(),
                                                          {keep_alive_tx_timeout}),

                    Keep_alive_rx_ref = erlang:send_after(?KEEP_ALIVE_RX_TIMER,
                                                          self(),
                                                          {keep_alive_rx_timeout}),

                    {noreply,
                     State#state{keep_alive_rx_ref=Keep_alive_rx_ref,
                                 keep_alive_tx_ref=Keep_alive_tx_ref,
                                 socket=Socket},
                     hibernate};
                {error, Reason} ->
                    lager:debug("~p: ~p: failed send handshake, reason: '~p'", [?MODULE, ?FUNCTION_NAME, Reason]),
                    {stop, normal, State}
            end;
        {error, Reason_connect} ->
            lager:debug("~p: ~p: peer_srv failed to establish connection with peer address: '~p', port: '~p', reason: '~p'",
                        [?MODULE,
                         ?FUNCTION_NAME,
                         State#state.address,
                         State#state.port,
                         Reason_connect]),

            {stop, normal, State}
    end;
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(Request, State) ->
    lager:warning("~p: ~p: unhandled request: '~p'", [?MODULE, ?FUNCTION_NAME, Request]),
    {noreply, State}.

%% Timeout for when a keep alive message was expected from the peer
handle_info({keep_alive_rx_timeout}, State) ->
    {stop, peer_worker_timed_out, State, hibernate};

%% Time to send another keep alive before the peer mark us as inactive
handle_info({keep_alive_tx_timeout}, State) ->
    case send_keep_alive(State#state.socket) of
        {ok, Timer_ref} ->
            New_state = State#state{keep_alive_tx_ref=Timer_ref},
            {noreply, New_state, hibernate};
        {error, Reason} ->
            {stop, Reason, State}
    end;

%% Async. response when requested a new piece to transmit. Answer the cached
%% request that triggered a piece to be retrieved from disk.
handle_info({peer_srv_tx_piece, Index, Hash, Data}, State) ->
    Outgoing_pieces = [{Index, Hash, Data}| State#state.outgoing_piece_queue],

    % Compile a list of pending requests that match the received piece
    Pending_requests = lists:filter(
                           fun({request, R_index, _R_begin, _R_length}) ->
                               case Index == R_index of
                                   true -> true;
                                   false -> false
                               end
                           end, State#state.request_buffer
                       ),

    % Respond to the pending requests and compile a list with the served
    % requests
    Served_requests = lists:foldl(
                          fun(Request, Acc) ->
                              {request, R_index, R_begin, R_length} = Request,

                              % TODO figure out how to determine when we have
                              % reached the last block of a piece in order to
                              % discard the cached data.
                              <<_B_begin:R_begin, Block:R_length, _Rest/binary>> = Data,

                              Msg = ?PEER_PROTOCOL:msg_piece(R_length, R_index, R_begin, Block),

                              case gen_tcp:send(State#state.socket, Msg) of
                                  ok ->
                                      [Request| Acc];
                                  {error, Reason} ->
                                      ?WARNING("failed to serve a pending request: " ++ Reason),
                                      Acc
                              end
                          end, [], Pending_requests
                      ),

    % Filter out the served requests
    Remaining_requests = lists:filter(
                            fun(Request) ->
                                case Request of
                                    {request, _R_index, _R_begin, _R_length} ->
                                        false;
                                    _ ->
                                        true
                                end
                            end, Served_requests
                         ),

    New_state = State#state{request_buffer = Remaining_requests,
                            outgoing_piece_queue = Outgoing_pieces},

    {noreply, New_state, hibernate};

%% Messages from gen_tcp
% TODO:
% - Implement the missing fast extension

%% @doc Handling incoming peer handshaked.
handle_info({tcp, _S, <<>>}, State) ->
    New_keep_alive_rx_ref = reset_rx_keep_alive(State#state.keep_alive_rx_ref),

    New_state = State#state{keep_alive_rx_ref=New_keep_alive_rx_ref},

    {noreply, New_state, hibernate};
handle_info({tcp, _S, <<Length:32/big-integer,
                        Message_id:8/big-integer,
                        Rest/binary>>}, State) ->
    % TODO change to one convertion of message id
    lager:debug("~p: ~p: length '~p', id '~p'",
                [?MODULE,
                 ?FUNCTION_NAME,
                 Length,
                 Message_id]),
    % Sorted by assumed frequency
    % TODO meassure the frequency to determine if the is correctly assumed or
    % if this will be causing issues (possible inefficient matching)
    case Message_id of
        ?REQUEST ->
            lager:debug("~p: ~p: message request", [?MODULE, ?FUNCTION_NAME]),
            <<Index:32/big, Begin:32/big, Length:32/big>> = Rest,
            lager:debug("index '~p', begin '~p', length '~p'", [Index, Begin, Length]),
            {ok, New_state} = handle_request(Index, Begin, Length, State);
        ?PIECE ->
            lager:debug("~p: ~p: message piece", [?MODULE, ?FUNCTION_NAME]),
            <<Index:32/big-integer, Begin:32/big-integer, Data/binary>> = Rest,
            {ok, New_state} = handle_piece(Index, Begin, Data, State);
        ?HAVE ->
            lager:debug("~p: ~p: message have", [?MODULE, ?FUNCTION_NAME]),
            <<Piece_index:32/big-integer>> = Rest,
            {ok, New_state} = handle_have(Piece_index, State);
        ?CANCEL ->
            lager:debug("~p: ~p: message cancel", [?MODULE, ?FUNCTION_NAME]),
            <<Index:32/big, Begin:32/big, Length:32/big>> = Rest,
            {ok, New_state} = handle_cancel(Index, Begin, Length, State);
        ?CHOKE ->
            lager:debug("~p: ~p: message choke", [?MODULE, ?FUNCTION_NAME]),
            {ok, New_state} = handle_choke(State);
        ?UNCHOKE ->
            lager:debug("~p: ~p: message uncoke", [?MODULE, ?FUNCTION_NAME]),
            {ok, New_state} = handle_unchoke(State);
        ?INTERESTED ->
            lager:debug("~p: ~p: message interested", [?MODULE, ?FUNCTION_NAME]),
            {ok, New_state} = handle_interested(State);
        ?NOT_INTERESTED ->
            lager:debug("~p: ~p: message not interested", [?MODULE, ?FUNCTION_NAME]),
            {ok, New_state} = handle_not_interested(State);
        ?BITFIELD ->
            lager:debug("~p: ~p: message bitfield", [?MODULE, ?FUNCTION_NAME]),
            <<Bitfield/binary>> = Rest,
            {ok, New_state} = handle_bitfield(Bitfield, State);
        ?PORT ->
            lager:debug("~p: ~p: message port", [?MODULE, ?FUNCTION_NAME]),
            <<Port:16/big>> = Rest,
            {ok, New_state} = handle_port(Port, State);
        % FAST EXTENSION
        ?SUGGEST_PIECE ->
            lager:debug("~p: ~p: message suggest piece", [?MODULE, ?FUNCTION_NAME]),
            New_state = State;
        ?HAVE_ALL ->
            lager:debug("~p: ~p: message have all", [?MODULE, ?FUNCTION_NAME]),
            New_state = State;
        ?HAVE_NONE ->
            lager:debug("~p: ~p: message have none", [?MODULE, ?FUNCTION_NAME]),
            New_state = State;
        ?REJECT_PIECE ->
            lager:debug("~p: ~p: message reject piece", [?MODULE, ?FUNCTION_NAME]),
            New_state = State;
        ?ALLOWED_FAST ->
            lager:debug("~p: ~p: message allowed fast", [?MODULE, ?FUNCTION_NAME]),
            New_state = State;
        _ ->
            lager:debug("~p: ~p: unhandled message id '~p'",
                        [?MODULE, ?FUNCTION_NAME, Message_id]),
            New_state = State
    end,
    {noreply, New_state, hibernate};
handle_info({tcp, _S, <<19/integer,
                        "BitTorrent protocol",
                        Flags_bin:8/bytes,
                        Info_hash:20/bytes,
                        Peer_id/bytes>>}, % Put the rest of the message into peer id, seen cases where this is 25 bytes and should be 20 bytes
             State)  ->
    lager:debug("~p: ~p: received a handshake,~nflags: '~p',~ninfo hash: '~p',~npeer id: '~p'",
                [
                 ?MODULE,
                 ?FUNCTION_NAME,
                 Flags_bin,
                 binary_to_list(Info_hash),
                 binary_to_list(Peer_id)
                ]),

    {ok, Flags} = parse_peer_flags(Flags_bin),
    lager:debug("~p: ~p: peer flags '~p'", [?MODULE, ?FUNCTION_NAME, Flags]),

    % TODO add validation of Peer_id
    case State#state.torrent_info_hash_bin == Info_hash of
        true when State#state.received_handshake =:= false ->
            New_state = State#state{received_handshake=true},
            {noreply, New_state, hibernate};
        true when State#state.received_handshake =:= true ->
            lager:info("~p: ~p: received multiple handshakes for peer: '~p'",
                       [?MODULE, ?FUNCTION_NAME, State#state.id]),
            {noreply, State, hibernate};
        false ->
            lager:warning("~p: ~p: received invalid info hash in handshake for peer: '~p'",
                          [?MODULE, ?FUNCTION_NAME, State#state.id]),
            {stop, "invalid handshake", State}
    end;

handle_info({tcp_closed, _S}, State) ->
    lager:debug("~p: ~p: peer closed the conncetion", [?MODULE, ?FUNCTION_NAME]),
    {stop, normal, State};
handle_info({tcp, _S, Message}, State) ->
    lager:warning("~p: ~p: peer '~p' received an unhandled tcp message: '~p'",
                  [?MODULE, ?FUNCTION_NAME, State#state.id, Message]),
    {stop, normal, State};
handle_info(Message, State) ->
    lager:warning("~p: ~p: peer '~p' received an unhandled message: '~p'",
                  [?MODULE, ?FUNCTION_NAME, State#state.id, Message]),
    {stop, normal, State}.
