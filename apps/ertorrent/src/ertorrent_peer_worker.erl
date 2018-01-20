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
         start_link/8,
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

-record(rx_data, {
                  dispatched::list(),
                  prepared::list(),
                  received::list()
                 }).

-record(retry, {
                max_attempts::integer(),
                queue::list(),
                timer::integer()
               }).

-record(state, {
                address,
                assigned_pieces::list(),
                block_length,
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
                mode:: rx | tx, % mode will dictate if the peer worker will request pieces from the peer.
                % Piece queue is a list containing tuples with information for each piece
                % {index, hash, data}
                % outgoing_piece_queue will hold all the requested pieces until
                % they're completely transfered
                outgoing_piece_queue::list(),
                peer_bitfield,
                peer_id::string(),
                peer_srv_pid::pid(),
                peer_choked::boolean(),
                peer_interested::boolean(),
                piece_length::integer(),
                port::integer(),
                received_handshake::boolean(),
                retry::#retry{},
                retry_ref,
                request_buffer::list(),
                rx_queue::list(),
                rx_data::#rx_data{},
                socket,
                self_choked::boolean(),
                self_interested::boolean(),
                sent_handshake::boolean(),
                state_incoming,
                state_outgoing,
                torrent_bitfield,
                torrent_info_hash,
                torrent_info_hash_bin,
                torrent_peer_id,
                torrent_pid::pid(),
                tx_queue::list()
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
-define(RETRY_TIMER, 1000).

-define(BINARY, ertorrent_binary_utils).
-define(PEER_SRV, ertorrent_peer_srv).
-define(PEER_PROTOCOL, ertorrent_peer_tcp_protocol).
-define(TORRENT_W, ertorrent_torrent_worker).
-define(UTILS, ertorrent_utils).

%%% Extended client API

% Instructing the peer worker to start leeching
connect(ID) ->
    gen_server:cast(ID, peer_w_connect).

%%% Standard client API
start_link(ID, Block_length, Mode, Info_hash, Peer_id, Piece_length, Socket,
           Torrent_pid) ->
    lager:debug("~p: ~p", [?MODULE, ?FUNCTION_NAME]),
    gen_server:start_link(?MODULE, [ID, Block_length, Mode, Info_hash, Peer_id,
                                    Piece_length, Socket, Torrent_pid],
                          [{hibernate_after, 2000}]).

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

    State#state.torrent_pid ! {peer_w_rx_bitfield, State#state.id, Bitfield},

    {ok, State}.

handle_have(Piece_idx, State) ->
    lager:debug("~p: ~p: HAVE, piece index: '~p'", [?MODULE, ?FUNCTION_NAME, Piece_idx]),

    New_bitfield = ?BINARY:set_bit(Piece_idx, 1, State#state.peer_bitfield),

    State#state.peer_srv_pid ! {peer_w_rx_bitfield_update, New_bitfield},
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
    lager:debug("~p:~p", [?MODULE, ?FUNCTION_NAME]),

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
    lager:debug("~p: ~p", [?MODULE, ?FUNCTION_NAME]),
    % Inform the other subsystems that this peer is choked
    % TODO I am adding this for the future if we want to display this in any way

    % Updating the peer state and clearing the request buffer
    New_state = State#state{peer_choked = true,
                            request_buffer = []},

    {ok, New_state}.
handle_unchoke(State) ->
    lager:debug("~p: ~p", [?MODULE, ?FUNCTION_NAME]),

    % Updating the peer state
    New_state = State#state{peer_choked = false},

    {ok, New_state}.
handle_interested(State) ->
    lager:debug("~p: ~p", [?MODULE, ?FUNCTION_NAME]),

    % Updating the peer state
    New_state = State#state{peer_interested = true},

    {ok, New_state}.
% TODO figure out how to handle not interested
handle_not_interested(State) ->
    lager:debug("~p: ~p", [?MODULE, ?FUNCTION_NAME]),

    % Updating the peer state
    New_state = State#state{peer_interested = false},

    {ok, New_state}.
handle_cancel(Index, Begin, Length, State) ->
    lager:debug("~p: ~p", [?MODULE, ?FUNCTION_NAME]),
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
    lager:debug("~p: ~p", [?MODULE, ?FUNCTION_NAME]),
    New_state = State#state{dht_port = Port},
    {ok, New_state}.

parse_peer_flags(Flags_bin) ->
    lager:debug("~p: ~p", [?MODULE, ?FUNCTION_NAME]),

    Flags_list = binary_to_list(Flags_bin),

    % nth is index 1-based
    Byte_seven = lists:nth(8, Flags_list),
    Fast_extension = 16#04,
    Got_fast_extension = Byte_seven band Fast_extension == Fast_extension,
    lager:debug("FAST_EXTENSION: '~p'", [Got_fast_extension]),

    Flags = [{fast_extension, Got_fast_extension}],
    {ok, Flags}.

prepare_request_selection(Rx_queue, Selection_size) ->
    % Ask for a subset of the blocks
    % TODO add setting to control the amount of requests in the pipeline
    Requests = lists:sublist(Rx_queue, Selection_size),

    Fun = fun(Socket, Request, State) ->
              send_request(Socket, Request, State)
          end,

    % Requests = [{Piece_index, Block_offset, Block_length}, ...]
    Selection = [{X,Y,Z} || X <- [Fun], Y <- Requests, Z <- [0]],

    {ok, Selection}.

send_bitfield(Socket, Bitfield) ->
    Bitfield_len = bit_size(Bitfield),

    {ok, Bitfield_msg} = ?PEER_PROTOCOL:msg_bitfield(Bitfield_len, Bitfield),

    gen_tcp:send(Socket, Bitfield_msg).

send_handshake(Socket, Peer_id_str, Torrent_info_bin) ->
    Peer_id_bin = list_to_binary(Peer_id_str),
    {ok, Handshake} = ?PEER_PROTOCOL:msg_handshake(Torrent_info_bin,
                                                   Peer_id_bin),
    gen_tcp:send(Socket, Handshake).

dispatch(Socket, Message) ->
    case gen_tcp:send(Socket, Message) of
        ok ->
            ok;
        {error, Reason} ->
            lager:debug("~p: ~p: reason '~p'", [?MODULE, ?FUNCTION_NAME, Reason]),
            error
    end.

send_interested(Socket) ->
    {ok, Interested_msg} = ?PEER_PROTOCOL:msg_interested(),

    case gen_tcp:send(Socket, Interested_msg) of
        ok ->
            lager:debug("~p: ~p: sent interested", [?MODULE, ?FUNCTION_NAME]),
            ok;
        {error, Reason} ->
            lager:warning("~p: ~p: failed to send interested, reason: '~p'",
                          [?MODULE, ?FUNCTION_NAME, Reason]),
            error
    end.

send_unchoke(Socket) ->
    {ok, Unchoke_msg} = ?PEER_PROTOCOL:msg_unchoke(),

    case gen_tcp:send(Socket, Unchoke_msg) of
        ok ->
            lager:debug("~p: ~p: sent unchoke", [?MODULE, ?FUNCTION_NAME]),
            ok;
        {error, Reason} ->
            lager:warning("~p: ~p: failed to send unchoke, reason: '~p'",
                          [?MODULE, ?FUNCTION_NAME, Reason]),
            error
    end.

send_request(Socket, {Piece_index, Begin, Length}, State) ->
    {ok, Message} = ?PEER_PROTOCOL:msg_request(Piece_index, Begin, Length),

    send_message(Socket, Message, State).

send_requests(State) ->
    lager:debug("~p: ~p", [?MODULE, ?FUNCTION_NAME]),

    Rx_data = State#state.rx_data,


    Foldl = fun(Entry, Acc) ->
                {Fun, Message, Attempts} = Entry,
                {Socket, Retry_list} = Acc,

                case Fun(Socket, Message, State) of
                    % Successfully sent the piece request
                    ok ->
                        New_retry_list = Retry_list;
                    error ->
                        lager:debug("~p: ~p: failed to send message to peer, terminating peer",
                                    [?MODULE, ?FUNCTION_NAME]),
                        New_retry_list = [error| Retry_list]
                end,

                {Socket, New_retry_list}
            end,
    {_Socket, New_retries} = lists:foldl(Foldl, {State#state.socket, []}, Rx_data#rx_data.prepared),

    New_dispatched = lists:concat(Rx_data#rx_data.dispatched,
                                  Rx_data#rx_data.prepared),
    New_rx_data = Rx_data#rx_data{prepared = []}

    % If _any_ of the retries result in an error, terminate the peer
    case lists:member(error, New_retries) of
        false ->
            {noreply, New_state, hibernate};
        true ->
            {stop, normal, State}
    end;


%%% Callback module
init([ID, Block_length, Mode, Info_hash, Peer_id, Piece_length, {Address, Port}, Torrent_pid])
      when is_integer(ID)
      andalso is_binary(Info_hash) ->
    lager:debug("~p: ~p: starting peer with \
                ID: '~p' \
                block length: '~p' \
                mode: '~p' \
                info hash: '~p' \
                peer id: '~p' \
                piece length: '~p'",
                [?MODULE, ?FUNCTION_NAME, ID, Block_length, Mode, Info_hash, Peer_id, Piece_length]),

    % Assigning default values for retries
    Retry = #retry{
                   max_attempts = 300,
                   queue = [],
                   timer = 100
                  },

    Rx_data = #rx_data{
                       dispatched = [],
                       prepared = [],
                       received = []
                      }

    {ok, #state{
                address = Address,
                assigned_pieces = [],
                id = ID,
                block_length = Block_length,
                mode = Mode,
                peer_id = Peer_id, % TODO Should this be retreived from settings_srv?
                peer_choked = true,
                peer_interested = false,
                piece_length = Piece_length,
                port = Port,
                received_handshake = false,
                retry = Retry,
                rx_data = Rx_data,
                rx_queue = [],
                self_choked = true,
                self_interested = false,
                sent_handshake = false,
                torrent_info_hash_bin = Info_hash,
                torrent_pid = Torrent_pid},
                hibernate
               }.

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
                               State#state.assigned_pieces},

    ok.

%% Synchronous
handle_call(_Req, _From, State) ->
    {noreply, State}.

%% Asynchronous
handle_cast(peer_w_connect, State) ->
    lager:debug("~p: ~p: connect", [?MODULE,
                                    ?FUNCTION_NAME]),

    case gen_tcp:connect(State#state.address,
                         State#state.port, [binary, {packet, 0}], 2000) of
        {ok, Socket} ->
            lager:debug("~p: ~p: connected to peer '~p:~p'", [?MODULE,
                                                              ?FUNCTION_NAME,
                                                              State#state.address,
                                                              State#state.port
                                                             ]),
            case send_handshake(Socket,
                                State#state.peer_id,
                                State#state.torrent_info_hash_bin) of
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
                     State#state{keep_alive_rx_ref = Keep_alive_rx_ref,
                                 keep_alive_tx_ref = Keep_alive_tx_ref,
                                 sent_handshake = true,
                                 socket = Socket},
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
    lager:debug("~p: ~p: peer timed out", [?MODULE, ?FUNCTION_NAME]),
    {stop, normal, State};

%% Time to send another keep alive before the peer mark us as inactive
handle_info({keep_alive_tx_timeout}, State) ->
    case send_keep_alive(State#state.socket) of
        {ok, Timer_ref} ->
            New_state = State#state{keep_alive_tx_ref=Timer_ref},
            {noreply, New_state, hibernate};
        {error, Reason} ->
            lager:debug("~p: ~p: failed to send timeout: '~p'", [?MODULE,
                                                                 ?FUNCTION_NAME,
                                                                 Reason]),
            {stop, normal, State}
    end;

% TODO when this is used make sure to cancel the retry reference
handle_info({message_retry_loop}, State) ->
    lager:debug("~p: ~p", [?MODULE, ?FUNCTION_NAME]),

    Old_retry = State#state.retry,
    Max_attempts = Old_retry#retry.max_attempts,

    Foldl = fun(Entry, Acc) ->
                {Fun, Message, Attempts} = Entry,
                {Socket, Retry_list} = Acc,

                case Fun(Socket, Message, State) of
                    % Successfully sent the piece request
                    ok ->
                        New_retry_list = Retry_list;
                    % Keep on retrying
                    not_ready when Attempts =< Max_attempts ->
                        New_retry_list = [{Fun, Message, Attempts + 1}| Retry_list];
                    % Exceeded the maximum amount of attempts for the request propagate
                    % error.
                    not_ready when Attempts > Max_attempts ->
                        lager:debug("~p: ~p: reached maximum amount of retries for message '~p', terminating peer",
                                    [?MODULE, ?FUNCTION_NAME, Message]),
                        New_retry_list = [error| Retry_list];
                    % Failed to transmit, propagate error
                    error ->
                        lager:debug("~p: ~p: failed to send message to peer, terminating peer",
                                    [?MODULE, ?FUNCTION_NAME]),
                        New_retry_list = [error| Retry_list]
                end,

                {Socket, New_retry_list}
            end,
    {_Socket, New_retries} = lists:foldl(Foldl, {State#state.socket, []}, Old_retry#retry.queue),

    New_retry = Old_retry#retry{queue = New_retries},

    % If _any_ of the retries result in an error, terminate the peer
    case lists:member(error, New_retries) of
        false ->
            % Check if theres any queued requests left, otherwise don't refresh the
            % timer.
            case length(New_retries) == 0 of
                true ->
                    New_state = State#state{retry = New_retry,
                                            retry_ref = undefined};
                false ->
                    Ref = erlang:send_after(?RETRY_TIMER, self(), {message_retry_loop}),
                    New_state = State#state{retry = New_retry,
                                            retry_ref = Ref}
            end,

            {noreply, New_state, hibernate};
        true ->
            {stop, normal, State}
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

handle_info({torrent_w_rx_pieces, Pieces}, State) ->
    lager:debug("~p: ~p: torrent_w_rx_pieces: '~p'", [?MODULE, ?FUNCTION_NAME, Pieces]),

    Self_interested_req = send_interested(State#state.socket),
    Self_choked_req = send_unchoke(State#state.socket),

    case Self_interested_req == ok andalso Self_choked_req == ok of
        true ->
            % Generate a tuple list with the block offsets and the length per block
            {ok, Block_offsets} = ?UTILS:block_offsets(State#state.block_length,
                                                       State#state.piece_length),

            % The block offsets is the same for each piece so lets put them together in
            % a tuplelist.
            Add_rx_queue = [{Piece_index, Block_offset, Block_length} ||
                            Piece_index <- Pieces,
                            {Block_offset, Block_length} <- Block_offsets],

            Old_retry = State#state.retry,
            Old_retry_queue = Old_retry#retry.queue,

            {ok, Selection} = prepare_request_selection(Add_rx_queue, 10),

            New_retry_queue = lists:merge(Selection, Old_retry_queue),

            Retry = Old_retry#retry{queue = New_retry_queue},

            New_rx_queue = State#state.rx_queue ++ Add_rx_queue,

            Rx_data = State#state.rx_data,
            Prepared = lists:merge(Rx_data.prepared, Selection),
            New_rx_data = Rx_data#rx_data{prepared = Prepared},

            New_state = State#state{assigned_pieces = Pieces,
                                    retry = Retry,
                                    rx_data = New_rx_data,
                                    rx_queue = New_rx_queue,
                                    self_choked = false,
                                    self_interested = true},

            {noreply, New_state, hibernate};
        false ->
            lager:debug("failed to send interest och unchoke", []),
            {stop, normal, State}
    end;

%% Messages from gen_tcp
% TODO:
% - Implement the missing fast extension

handle_info({tcp, _S, <<>>}, State) ->
    New_keep_alive_rx_ref = reset_rx_keep_alive(State#state.keep_alive_rx_ref),

    New_state = State#state{keep_alive_rx_ref=New_keep_alive_rx_ref},

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

    case State#state.torrent_info_hash_bin == Info_hash of
        true when State#state.received_handshake =:= false ->
            Socket = State#state.socket,

            lager:debug("MODE: '~p'", [State#state.mode]),

            Request_rx_pieces = fun(S) ->
                                    case S#state.mode of
                                        % TODO the size of the rx piece queue
                                        % should be control from somewhere else
                                        rx -> ?TORRENT_W:request_rx_pieces(S#state.torrent_pid,
                                                                           S#state.peer_bitfield,
                                                                           3);
                                        tx -> ok
                                    end
                                end,

            case State#state.sent_handshake of
                true ->
                    Handshake = ok;
                false ->
                    Handshake = send_handshake(Socket,
                                               State#state.peer_id,
                                               State#state.torrent_info_hash_bin)
            end,

            % Send bitfield
            Bitfield = ?TORRENT_W:get_bitfield(State#state.torrent_pid),
            lager:debug("OWN BITFIELD length '~p' '~p'", [bit_size(Bitfield), Bitfield]),

            case Handshake == ok andalso send_bitfield(Socket, Bitfield) of
                ok ->
                    lager:debug("~p: ~p: requested rx pieces", [?MODULE, ?FUNCTION_NAME]),
                    Request_rx_pieces(State),
                    New_state = State#state{received_handshake = true,
                                            sent_handshake = true},
                    {noreply, New_state, hibernate};
                false ->
                    {error, Reason} = Handshake,
                    lager:debug("~p: ~p: failed to send handshake '~p'",
                                [?MODULE, ?FUNCTION_NAME, Reason]),
                    {stop, normal, State};
                {error, Reason} ->
                    lager:debug("~p: ~p: failed to send bitfield '~p'",
                                [?MODULE, ?FUNCTION_NAME, Reason]),
                    {stop, normal, State}
            end;
        true when State#state.received_handshake =:= true ->
            lager:info("~p: ~p: received multiple handshakes for peer: '~p'",
                       [?MODULE, ?FUNCTION_NAME, State#state.id]),
            {noreply, State, hibernate};
        false ->
            lager:warning("~p: ~p: received invalid info hash in handshake for peer: '~p'",
                          [?MODULE, ?FUNCTION_NAME, State#state.id]),
            {stop, normal, State}
    end;
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
            lager:debug("~p: ~p: message unchoke", [?MODULE, ?FUNCTION_NAME]),
            {ok, New_state} = handle_unchoke(State);
        ?INTERESTED ->
            lager:debug("~p: ~p: message interested", [?MODULE, ?FUNCTION_NAME]),
            {ok, New_state} = handle_interested(State);
        ?NOT_INTERESTED ->
            lager:debug("~p: ~p: message not interested", [?MODULE, ?FUNCTION_NAME]),
            {ok, New_state} = handle_not_interested(State);
        ?BITFIELD ->
            lager:debug("~p: ~p: message bitfield length '~p', bitfield '~p'",
                        [?MODULE, ?FUNCTION_NAME, bit_size(Rest), Rest]),
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
            lager:debug("~p: ~p: unhandled message id '~p', handshake '~p', request '~p'",
                        [?MODULE, ?FUNCTION_NAME, Message_id, State#state.received_handshake, Rest]),
            New_state = State
    end,
    {noreply, New_state, hibernate};

handle_info({tcp_closed, _S}, State) ->
    lager:debug("~p: ~p: peer closed the connection", [?MODULE, ?FUNCTION_NAME]),
    {stop, normal, State};
handle_info({tcp, _S, Message}, State) ->
    lager:warning("~p: ~p: peer '~p' received an unhandled tcp message: '~p'",
                  [?MODULE, ?FUNCTION_NAME, State#state.id, Message]),
    {stop, normal, State};
handle_info(Message, State) ->
    lager:warning("~p: ~p: peer '~p' received an unhandled message: '~p'",
                  [?MODULE, ?FUNCTION_NAME, State#state.id, Message]),
    {stop, normal, State}.
