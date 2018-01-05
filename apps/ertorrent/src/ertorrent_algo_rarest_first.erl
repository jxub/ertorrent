-module(ertorrent_algo_rarest_first).

-export([
         order_rx_pieces/2,
         create_piece_queue/5
        ]).

-define(BINARY, ertorrent_binary_utils).
-define(UTILS, ertorrent_utils).

initial_rx_pieces() ->


% Returns a sorted tuple list with the rarest piece being the head of the list.
% E.g. [{Piece_index, Piece_occurrences}]
order_rx_pieces(Peer_bitfields, Own_bitfield) when is_list(Peer_bitfields) andalso
                                                   is_list(Own_bitfield)->

    lager:debug("~p: ~p: peer bitfields '~p'", [?MODULE, ?FUNCTION_NAME, Peer_bitfields]),
    lager:debug("~p: ~p: own bitfield '~p'", [?MODULE, ?FUNCTION_NAME, Own_bitfield]),

    % Filter peer's id from the tuple list Peer_bitfields
    Bitfields_alone = [Bitfield || {_Peers_id, Bitfield} <- Peer_bitfields],

    % Compile all the bitfields into one list to visualize the rarest pieces
    {ok, Bitfields_sum} = ?BINARY:sum_bitfields(Bitfields_alone),

    % Add indices to the bitfield summary to keep track of the sum for each
    % index before sorting.
    {ok, Sum_with_idx} = ?UTILS:index_list(Bitfields_sum),

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

create_piece_queue(Peer_bitfield,
                   Prioritized_pieces,
                   Distributed_pieces,
                   Piece_distribution_limit,
                   Piece_queue_limit) ->
    lager:debug("~p: ~p", [?MODULE, ?FUNCTION_NAME]),

    % Compile a list with pieces that are currently distributed
    Limited_pieces = [Piece_index || {Piece_index, Distributed} <-
                                     Distributed_pieces, Distributed <
                                     Piece_distribution_limit],

    % Filter out the pieces that are already assigned to other peer workers
    WO_limited_pieces = lists:filter(fun({Piece_index, _Occurrences}) ->
                                         Tmp = not lists:member(Piece_index, Limited_pieces),
                                         lager:debug("TMP: '~p'", [Tmp]),
                                         Tmp
                                     end, Prioritized_pieces),

    % Find the remaining pieces which the peer possess
    Available_pieces = lists:filtermap(fun({Index, _Occurrences}) ->
                                           % Check if the peer possess the most relevant piece
                                           case lists:nth(Index, Peer_bitfield) of
                                               1 -> true;
                                               0 -> false
                                           end
                                       end, WO_limited_pieces),

    % Apply the queue limit
    Pieces_queue_limit = lists:sublist(Available_pieces, Piece_queue_limit),

    % Remove Occurrences from the response
    Pieces = [Piece_index || {Piece_index, _Occurrences} <- Pieces_queue_limit],

    % Update the list of Distributed_pieces with Pieces
    % Start by filter out the tuples for the outdated pieces
    Map = fun({Distrib_index, Distrib_times}, Acc) ->
              case list:memember(Distrib_index, Acc) of
                  true ->
                      New_acc = lists:delete(Acc),
                      {{Distrib_index, Distrib_times + 1}, New_acc};
                  false ->
                      {{Distrib_index, Distrib_times}, Acc}
              end
          end,
    % Increase the counter of the existing indices and leave the missing
    % indices in the accumulator.
    {Distrib_pieces_tmp, Missing_indices} = lists:mapfoldl(Map, Pieces, Distributed_pieces),

    % Create a tuple list with the missing indices and the occurrence.
    Formated_indices = [{X, 1} || X <- Missing_indices],

    % Concatenate the tuple list with the existing and the new piece indices.
    New_distrib_pieces = Distrib_pieces_tmp ++ Formated_indices,

    Distrib_pieces_sorted = lists:keysort(1, New_distrib_pieces),

    {ok, Pieces, Distrib_pieces_sorted}.
