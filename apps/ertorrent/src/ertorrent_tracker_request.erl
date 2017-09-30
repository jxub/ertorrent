-module(ertorrent_tracker_request).

-export([
         new_request/8,
         new_request/9
        ]).

new_request(Announce_address, Info_hash, Peer_id, Port, Uploaded, Downloaded,
            Left, Event) ->
    lists:concat([Announce_address, "?", "info_hash=",
                  Info_hash, "&peer_id=", Peer_id, "&port=", Port,
                  "&uploaded=", Uploaded, "&downloaded=", Downloaded, "&left=",
                  Left, "&event=", Event]).

new_request(Announce_address, Info_hash, Peer_id, Port, Uploaded, Downloaded,
            Left, Event, Compact) ->
    lists:concat([Announce_address, "?", "info_hash=",
                  Info_hash, "&peer_id=", Peer_id, "&port=", Port,
                  "&uploaded=", Uploaded, "&downloaded=", Downloaded, "&left=",
                  Left, "&event=", Event, "&compact=", Compact]).
