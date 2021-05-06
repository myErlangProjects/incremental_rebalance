%%%-------------------------------------------------------------------
%% @doc incremental_rebalance default callback module.
%% @end
%%%-------------------------------------------------------------------

-module(incremental_rebalance_default_callback).
-author('Chanaka Fernando <contactchanaka@gmail.com>').
-export([onResourceRevoked/1, onResourceAssigned/1]).

onResourceRevoked(ResourceData) ->
    error_logger:info_msg("onResourceRevoked : ~p~n", [ResourceData]).

onResourceAssigned(ResourceData) ->
    error_logger:info_msg("onResourceAssigned : ~p~n", [ResourceData]).