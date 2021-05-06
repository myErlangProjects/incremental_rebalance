%%%-------------------------------------------------------------------
%% @doc incremental_rebalance top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(incremental_rebalance_sup).
-author('Chanaka Fernando <contactchanaka@gmail.com>').

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1},
    Chroot	= application:get_env(incremental_rebalance, 'zk.chroot', "/zk"),
    ServerName = node(),
    ChildSpecs = [{incremental_rebalance_svr,{gen_server, start_link,[{local,incremental_rebalance_svr},incremental_rebalance_svr,[ServerName, Chroot],[]]},
                    permanent, 10000, worker, [incremental_rebalance_svr]
                }],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
