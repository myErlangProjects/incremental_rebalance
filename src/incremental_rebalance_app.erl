%%%-------------------------------------------------------------------
%% @doc incremental_rebalance public API
%% @end
%%%-------------------------------------------------------------------

-module(incremental_rebalance_app).
-author('Chanaka Fernando <contactchanaka@gmail.com>').

-behaviour(application).

-export([start/2, stop/1]).

start(normal = _StartType, _Args) ->
	case incremental_rebalance_sup:start_link() of
		{ok, Sup} ->
			{ok, Sup};
		{error, Reason} ->
			{error, Reason}
	end.

stop(_State) ->
    ok.

%% internal functions
