-module(ertorrent_algo_rarest_first).

-export([construct_piece_order/2,
         create_piece_pipe/5]).

-define(BINARY, ertorrent_binary_utils).
-define(UTILS, ertorrent_utils).

% Returns a sorted tuple list with the rarest piece being the head of the list.
% E.g. [{Piece_index, Piece_occurrences}]
construct_piece_order(Peer_bitfields, Own_bitfield) when is_list(Peer_bitfields) andalso
                                                         is_list(Own_bitfield)->
    % Filter peer's id from the tuple list Peer_bitfields
    Bitfields_alone = [Bitfield || {_Peers_id, Bitfield}=_L <- Peer_bitfields],

    % Compile all the bitfields into one list to visualize the rarest pieces
    Bitfields_sum = ?BINARY:sum_bitfields(Bitfields_alone),

    % Add indices to the bitfield summary to keep track of the sum for each
    % index before sorting.
    Sum_with_idx = ?UTILS:index_list(Bitfields_sum),

    % Sorting the summary
    Rarest_first = lists:keysort(2, Sum_with_idx),

    % Zip with the own bitfield
    Zipped = lists:zipwith(fun({Index, Occurrences}, Bit) ->
                               {Index, Occurrences, Bit}
                           end, Rarest_first, Own_bitfield),

    % Filter the piece that has already been downloaded
    Filtered = lists:filtermap(fun({Index, Occurrences, Bit}) ->
                                   case Bit of
                                       % Keep the original tuple if the piece is missing
                                       0 -> {true, {Index, Occurrences}};
                                       % Discard the tuple if the piece already been downloaded
                                       1 -> false
                                   end
                               end, Zipped),

    {ok, Filtered}.

create_piece_pipe(Peer_bitfield,
                  Prioritized_pieces,
                  Distributed_pieces,
                  Piece_distribution_limit,
                  Piece_pipe_limit) ->
    lager:debug("~p: ~p", [?MODULE, ?FUNCTION_NAME]),

    % Compile a list with pieces that are currently distributed
    Limited_pieces = [Piece_index || {Piece_index, Distributed} <-
                                     Distributed_pieces, Distributed <
                                     Piece_distribution_limit],

    % Filter out the pieces that are already assigned to other peer workers
    WO_limited_pieces = lists:filter(fun({Piece_index, _Occurences}) ->
                                         not lists:member(Piece_index, Limited_pieces)
                                     end, Prioritized_pieces),

    Available_pieces = lists:filtermap(fun({Index, _Occurrences}) ->
                                           % Check if the peer posess the most relevant piece
                                           case lists:nth(Index, Peer_bitfield) of
                                               1 -> true;
                                               0 -> false
                                           end
                                       end, WO_limited_pieces),

    % Apply the pipe limit
    Pieces = lists:sublist(Available_pieces, Piece_pipe_limit),

    {ok, Pieces}.
