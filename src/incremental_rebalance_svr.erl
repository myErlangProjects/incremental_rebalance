%%%-------------------------------------------------------------------
%% @doc incremental_rebalance worker.
%% @end
%%%-------------------------------------------------------------------

-module(incremental_rebalance_svr).
-author('Chanaka Fernando <contactchanaka@gmail.com>').

-behaviour(gen_server).

%% export the incremental_rebalance_svr API

%% export the callbacks needed for gen_server behaviour
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
			terminate/2, code_change/3]).	

%%-type state() :: #state{}.	
-record(state, {svr_name :: undefined | string(),
				zk_chroot :: undefined | string(),
				zk_connection :: undefined | pid(),
				zk_host :: undefined | string(),
				zk_znode :: undefined | binary(),
				zk_chroot_children :: undefined | list(),
				zk_znode_suffix :: undefined | list(),
				zk_adj_leader :: undefined | binary(),
				zk_revoke_candidates :: undefined | list(),
				zk_assign_candidates :: undefined | list(),
				local_resource_list :: undefined | list(),
				resource_list :: undefined | list(),
				rebalance_callback :: undefined | atom(),
				group_instance_id :: undefined | string(),
				role :: undefined | integer()
			}).

%%----------------------------------------------------------------------
%%  The incremental_rebalance_svr Macros
%%----------------------------------------------------------------------
-define(LEADER, 1).		%% Leader and Coordinator
-define(FOLLOWER, 0).	%% Follower
%%----------------------------------------------------------------------
%%  The incremental_rebalance_svr gen_server callbacks
%%----------------------------------------------------------------------

-spec init(Args :: [term()]) ->
	{ok, State :: #state{}}
			| {ok, State :: #state{}, Timeout :: timeout()}
			| {stop, Reason :: term()} | ignore.
%% @doc Initialize the {@module} server.
%% @see //stdlib/gen_server:init/1
%% @private
%%
init([SvrName, Chroot]) ->
	process_flag(trap_exit, true),
	erlzk:start(),
	ZkHost = application:get_env(incremental_rebalance, 'zk.host',"127.0.0.1"),
	DccLinks = application:get_env(incremental_rebalance, 'resource.list',[]),
	Callback = application:get_env(incremental_rebalance, 'rebalance.callback', incremental_rebalance_default_callback),
	{Mega,Sec,Milli} = os:timestamp(),
	DefaultInstantId = lists:concat([integer_to_list(N) || N <- [Mega,Sec,Milli]]),
	InstanceId = application:get_env(incremental_rebalance, 'group.instance.id', DefaultInstantId),
	{ok, Pid} = erlzk:connect([{ZkHost, 2181}], 30000,[{monitor, self()}]),
	link(Pid),
	error_logger:info_msg("Starting : ~p ~n", [SvrName]),
	{ok, #state{rebalance_callback = Callback, group_instance_id = InstanceId, svr_name = SvrName, zk_connection = Pid, zk_chroot = Chroot, zk_host = ZkHost, resource_list = DccLinks}}.											
	

-spec handle_call(Request :: term(), From :: {pid(), Tag :: any()},
		State :: #state{}) ->
	{reply, Reply :: term(), NewState :: #state{}}
			| {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate}
			| {noreply, NewState :: #state{}}
			| {noreply, NewState :: #state{}, timeout() | hibernate}
			| {stop, Reason :: term(), Reply :: term(), NewState :: #state{}}
			| {stop, Reason :: term(), NewState :: #state{}}.
%% @doc Handle a request sent using {@link //stdlib/gen_server:call/2.
%% 	gen_server:call/2,3} or {@link //stdlib/gen_server:multi_call/2.
%% 	gen_server:multi_call/2,3,4}.
%% @see //stdlib/gen_server:handle_call/3
%% @private
%%
	
handle_call(_Request, _From, State) ->
	{noreply, State}.

handle_cast({stop_creset_zk_conn}, #state{zk_connection = Pid} = State) ->
	exit(Pid, kill),
	{noreply, State};
	
handle_cast(stop, State) ->
	{stop, normal, State};
	
handle_cast(_Request, State) ->
	{noreply, State}.
	

-spec handle_info(Info :: timeout | term(), State::#state{}) ->
	{noreply, NewState :: #state{}}
			| {noreply, NewState :: #state{}, timeout() | hibernate}
			| {stop, Reason :: term(), NewState :: #state{}}.
%% @doc Handle a received message.
%% @see //stdlib/gen_server:handle_info/2
%% @private
%%
	

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%% LEADER messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_info(timeout, #state{role = ?LEADER, zk_znode_suffix = ZnodeSuffix} = State) ->
	error_logger:info_msg("LEADER: proceed_to_rebalance : ~p ~n", [ZnodeSuffix]),
	proceed_to_rebalance(ZnodeSuffix, State);
handle_info({node_children_changed, ChrootBin}, 
		#state{svr_name = SvrName,zk_connection = Pid,zk_znode = Znode,zk_chroot = Chroot, role = ?LEADER} = State) ->
	error_logger:info_msg("node_children_changed : ~p|~p : ({node_children_changed, Chroot}  : ~p ~n", [SvrName, Znode, {node_children_changed, ChrootBin}]),
	{ok, Children} = erlzk:get_children(Pid, Chroot),  %% Remove watcher
	NewState = State#state{zk_chroot_children = lists:sort(Children)},
	{noreply,NewState, 0};
handle_info({node_deleted, DelZbode}, 
	#state{svr_name = SvrName,zk_connection = Pid,zk_chroot = Chroot, 
		zk_znode = Znode, role = ?LEADER} = State) ->
	error_logger:info_msg("[~p] LEADER : ~p : {node_deleted,DelZbode} : ~p ~n", [SvrName, Znode, {node_deleted,DelZbode}]),
	{ok, Children} = erlzk:get_children(Pid, Chroot),
	NewState = State#state{zk_chroot_children = lists:sort(Children)},
	{noreply, NewState, 0};
handle_info({node_data_changed, Znode}, 
		#state{svr_name = SvrName,zk_chroot = Chroot, zk_znode = Znode, zk_chroot_children = ChildZNodes,zk_connection = Pid,
			 zk_revoke_candidates = [], zk_assign_candidates = [], local_resource_list = PData, role = ?LEADER} = State) ->
	{ok, {Data, _}} = erlzk:get_data(Pid, Znode),
	[{'group.instance.id', InstantId},{'instance.data', NData}] = binary_to_term(Data),
	error_logger:info_msg("[~p] LEADER :[1] ~p : {node_data_changed, Znode} : ~p ~n Data :~p~n", [SvrName, Znode, {node_data_changed, Znode},
	[{'group.instance.id', InstantId},{'instance.data', NData}]]),
	RvkLinks = PData -- NData,
	AsgLinks = NData -- PData,
	if  RvkLinks /= [] -> 
			error_logger:info_msg("[~p] LEADER Revoke called : Revoke resources : ~p~n", [SvrName, RvkLinks]),
			%% Revoke internal process
			(State#state.rebalance_callback):onResourceRevoked([{'group.instance.id', InstantId},{'instance.data', NData}]),
			erlzk:set_data(Pid, Znode, Data, -1);
		true ->
			if  AsgLinks /= [] -> 
				error_logger:info_msg("[~p] LEADER Assign called : Assign resources : ~p~n", [SvrName, AsgLinks]),
				%% Assign internal process
				(State#state.rebalance_callback):onResourceAssigned([{'group.instance.id', InstantId},{'instance.data', NData}]);
			true ->
				error_logger:info_msg("[~p] LEADER No change resources~n", [SvrName])
			end
	end,
	error_logger:info_msg("[~p] LEADER RESOURCES : ~p~n", [SvrName, NData]),
	erlzk:get_data(Pid, Znode, self()),
	{ok, Children0} = erlzk:get_children(Pid, Chroot),
	case lists:sort(Children0) of
		ChildZNodes ->
			erlzk:get_children(Pid, Chroot, self()), %% Add watcher
			NewState = State#state{local_resource_list = NData},
			{noreply, NewState};
		Children -> %% children changed
			error_logger:info_msg("[~p] LEADER leader_election : ~p~n", [SvrName, Children]),
			NewState = State#state{zk_revoke_candidates = [], zk_assign_candidates = [], local_resource_list = NData, zk_chroot_children = Children},
			{noreply, NewState, 0}
	end;
handle_info({node_data_changed, Znode}, 
		#state{svr_name = SvrName,zk_chroot = Chroot, zk_znode = Znode, zk_znode_suffix = ZnodeSuffix,
			zk_chroot_children = ChildZNodes,zk_connection = Pid, zk_revoke_candidates = RvkCandidates, 
				zk_assign_candidates = AsgCandidates, local_resource_list = PData, role = ?LEADER} = State) ->
	{ok, {Data, _}} = erlzk:get_data(Pid, Znode),
	[{'group.instance.id', InstantId},{'instance.data', NData}] = binary_to_term(Data),
	error_logger:info_msg("[~p] LEADER :[2] ~p : {node_data_changed, Znode} : ~p ~n Data :~p~n", [SvrName, Znode, {node_data_changed, Znode},
	[{'group.instance.id', InstantId},{'instance.data', NData}]]),
	RvkLinks = PData -- NData,
	AsgLinks = NData -- PData,
	if  RvkLinks /= [] -> 
			error_logger:info_msg("[~p] LEADER Revoke called : Revoke resources : ~p~n", [SvrName, RvkLinks]),
			%% Revoke internal process
		    (State#state.rebalance_callback):onResourceRevoked([{'group.instance.id', InstantId},{'instance.data', NData}]),
			erlzk:set_data(Pid, Znode, Data, -1);
		true ->
			if  AsgLinks /= [] -> 
				error_logger:info_msg("[~p] LEADER Assign called : Assign resources : ~p~n", [SvrName, AsgLinks]),
				%% Assign internal process
				(State#state.rebalance_callback):onResourceAssigned([{'group.instance.id', InstantId},{'instance.data', NData}]);
			true ->
				error_logger:info_msg("[~p] LEADER No change resources~n",[SvrName])
			end
	end,
	erlzk:get_data(Pid, Znode, self()),
	error_logger:info_msg("[~p] LEADER RESOURCES : ~p~n",[SvrName, NData]),
	NewRvkCandidates = lists:keydelete(ZnodeSuffix, 2, RvkCandidates),
	if 
		NewRvkCandidates == [] ->
			[erlzk:set_data(Pid, Chroot ++ "/" ++ Z, term_to_binary([{'group.instance.id', I},{'instance.data', D}]), -1)
			|| {I, Z, D} <- AsgCandidates],
			case lists:keymember(ZnodeSuffix, 2, AsgCandidates) of
				true ->
					NewState = State#state{zk_revoke_candidates = [], zk_assign_candidates = [], local_resource_list = NData},
					{noreply, NewState};
				_ ->
					{ok, Children0} = erlzk:get_children(State#state.zk_connection, State#state.zk_chroot),
					case  lists:sort(Children0) of
						ChildZNodes ->
							erlzk:get_children(State#state.zk_connection, State#state.zk_chroot, self()), %% add watcher
							NewState = State#state{zk_revoke_candidates = [], zk_assign_candidates = [], local_resource_list = NData},
							{noreply, NewState};
						Children ->
							error_logger:info_msg("[~p] LEADER leader_election  : ~p ~n", [SvrName, Children]),
							NewState = State#state{zk_revoke_candidates = [], zk_assign_candidates = [], local_resource_list = NData, zk_chroot_children = Children},
							{noreply, NewState, 0}
					end
			end;
		true ->
			NewState = State#state{zk_revoke_candidates = NewRvkCandidates, zk_assign_candidates = AsgCandidates, local_resource_list = NData},
			{noreply, NewState}
	end;
handle_info({node_data_changed, FZnode}, 
	#state{svr_name = SvrName,zk_chroot = Chroot, zk_znode = _Znode, zk_znode_suffix = ZnodeSuffix,
		zk_chroot_children = ChildZNodes,zk_connection = Pid, zk_revoke_candidates = RvkCandidates, 
			zk_assign_candidates = AsgCandidates, role = ?LEADER} = State) ->
	[_, FZnodeSuffix] = string:tokens(binary_to_list(FZnode), "/"),
	NewRvkCandidates = lists:keydelete(FZnodeSuffix, 2, RvkCandidates),
	if 
		NewRvkCandidates == [] ->
			[erlzk:set_data(Pid, Chroot ++ "/" ++ Z, term_to_binary([{'group.instance.id', I},{'instance.data', D}]), -1)
			|| {I, Z, D} <- AsgCandidates],
			case lists:keymember(ZnodeSuffix, 2, AsgCandidates) of
				true ->
					NewState = State#state{zk_revoke_candidates = [], zk_assign_candidates = []},
					{noreply, NewState};
				_ ->
					{ok, Children0} = erlzk:get_children(State#state.zk_connection, State#state.zk_chroot),
					case  lists:sort(Children0) of
						ChildZNodes ->
							erlzk:get_children(State#state.zk_connection, State#state.zk_chroot, self()), %% add watcher
							NewState = State#state{zk_revoke_candidates = [], zk_assign_candidates = []},
							{noreply, NewState};
						Children ->
							error_logger:info_msg("[~p] LEADER leader_election  : ~p ~n", [SvrName, Children]),
							NewState = State#state{zk_revoke_candidates = [], zk_assign_candidates = [], zk_chroot_children = Children},
							{noreply, NewState, 0}
					end
			end;
		true ->
			NewState = State#state{zk_revoke_candidates = NewRvkCandidates, zk_assign_candidates = AsgCandidates},
			{noreply, NewState}
	end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%% FOLLOWER messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_info(timeout, #state{role = ?FOLLOWER, zk_znode_suffix = ZnodeSuffix} = State) ->
	error_logger:info_msg("FOLLOWER : ignore proceed_to_rebalance : ~p ~n", [ZnodeSuffix]),
	{noreply, State};
handle_info({node_deleted,AdjLeader}, 
		#state{svr_name = SvrName,zk_connection = Pid,zk_chroot = Chroot, 
			zk_znode = Znode, zk_adj_leader = AdjLeader, role = ?FOLLOWER} = State) ->
	error_logger:info_msg("node_deleted : ~p|~p : {node_deleted,AdjLeader} : ~p ~n", [SvrName, Znode, {node_deleted,AdjLeader}]),
	{ok, Children} = erlzk:get_children(Pid, Chroot),
	NewState = leader_election(State#state{zk_chroot_children = lists:sort(Children)}),
	{noreply, NewState, 0};
handle_info({node_data_changed, Znode}, 
	#state{svr_name = SvrName,zk_znode = Znode,zk_connection = Pid, local_resource_list = PData, role = ?FOLLOWER} = State) ->
	{ok, {Data, _}} = erlzk:get_data(Pid, Znode),
	[{'group.instance.id', InstantId},{'instance.data', NData}] = binary_to_term(Data),
	RvkLinks = PData -- NData,
	AsgLinks = NData -- PData,
	if  RvkLinks /= [] -> 
			error_logger:info_msg("[~p] FOLLOWER Revoke called : Revoke resources : ~p~n", [SvrName,RvkLinks]),
			%% Revoke internal process
			(State#state.rebalance_callback):onResourceRevoked([{'group.instance.id', InstantId},{'instance.data', NData}]),
			erlzk:set_data(Pid, Znode, Data, -1);	%% redundant set_data to inform leader that revoke is done.
		true ->
			if  AsgLinks /= [] -> 
				error_logger:info_msg("[~p] FOLLOWER Assign called : Assign resources : ~p~n", [SvrName,AsgLinks]),
				%% Assign internal process
				(State#state.rebalance_callback):onResourceAssigned([{'group.instance.id', InstantId},{'instance.data', NData}]);
			true ->
				error_logger:info_msg("[~p] FOLLOWER No change resources~n", [SvrName])
			end
	end,
	error_logger:info_msg("[~p] FOLLOWER RESOURCES : ~p~n", [SvrName, NData]),
	erlzk:get_data(Pid, Znode, self()),
	{noreply, State#state{local_resource_list = NData}};
handle_info({node_data_changed, AdjLeader}, #state{zk_adj_leader = AdjLeader, zk_connection = Pid, role = ?FOLLOWER} = State) ->
	erlzk:exists(Pid, AdjLeader, self()),
	{noreply, State};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%% COMMON messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_info({disconnected, Host, Port}, #state{svr_name = SvrName} = State) ->
	error_logger:info_msg("ZK session disconnected : ~p|~p : Info : ~p~n", [SvrName, Host, Port]),
	{noreply, State};
handle_info({expired, Host, Port}, #state{svr_name = SvrName} = State) ->
	error_logger:info_msg("ZK session expired : ~p|~p : Info : ~p~n", [SvrName, Host, Port]),
	{noreply, State};
handle_info({connected, Host, Port}, #state{svr_name = SvrName, zk_znode = undefined} = State) ->
	error_logger:info_msg("Initial ZK session connected : ~p|~p : Info : ~p~n", [SvrName, Host, Port]),
	NewState = initiate_session(State),
	{noreply, NewState, 0};
handle_info({connected, Host, Port}, #state{svr_name = SvrName, zk_connection = Pid, zk_znode = Znode} = State) ->
	error_logger:info_msg("ZK session re-connected : ~p|~p : Info : ~p~n", [SvrName, Host, Port]),
	erlzk:delete(Pid, Znode),
	NewState = initiate_session(State),
	{noreply, NewState, 0};
handle_info({node_deleted,Znode}, #state{svr_name = SvrName,zk_znode = Znode} = State) ->
	error_logger:info_msg("node_deleted : ~p|~p : {node_deleted,Znode} : ~p ~n", [SvrName, Znode, {node_deleted,Znode}]),
	NewState = initiate_session(State),
	{noreply, NewState, 0};
handle_info({'EXIT', OldPid, Reason}, #state{svr_name = SvrName,zk_host = ZkHost, zk_znode = Znode} = State) when is_pid(OldPid) ->
	error_logger:error_msg("ZK Conn proc down: ~p|~p : Info : ~p~n", [SvrName, Znode, {'EXIT', OldPid, Reason}]),
	{ok, Pid} = erlzk:connect([{ZkHost, 2181}], 30000,[{monitor, self()}]),
	link(Pid),
	{noreply, State#state{zk_connection = Pid}};

handle_info(Info, #state{svr_name = SvrName,zk_znode = Znode, zk_connection = Pid} = State) ->
	error_logger:warning_msg("UNKNOWN msg received : ~p|~p : Info : ~p~n", [SvrName, Znode, Info]),
	erlzk:exists(Pid, Znode, self()),
	{noreply, State}.

-spec terminate(Reason :: normal | shutdown | {shutdown, term()} | term(),
		State::#state{}) ->
	any().
%% @doc Cleanup and exit.
%% @see //stdlib/gen_server:terminate/3
%% @private
%%
terminate(Reason, #state{svr_name = SvrName, zk_connection = Pid, zk_znode = Znode}=State) ->
	erlzk:delete(Pid, Znode),
	erlzk:close(Pid),
	error_logger:warning_msg("Terminating : ~p | Reason : ~p | State : ~p~n", [SvrName, Reason, State]),
	ok.

-spec code_change(OldVsn :: term() | {down, term()}, State :: #state{},
		Extra :: term()) ->
	{ok, NewState :: #state{}} | {error, Reason :: term()}.
%% @doc Update internal state local_resource_list during a release upgrade&#047;downgrade.
%% @see //stdlib/gen_server:code_change/3
%% @private
%%
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%----------------------------------------------------------------------
%%  internal functions
%%----------------------------------------------------------------------
initiate_session(#state{zk_connection = Pid, zk_chroot = Chroot, group_instance_id= InstantId} = State) ->
	ZnodeName = application:get_env(incremental_rebalance, 'zk.znode',"resource"),
	erlzk:create(Pid, Chroot, persistent),
	Data = [{'group.instance.id', InstantId},{'instance.data', []}],
	{ok, Znode} = erlzk:create(Pid, Chroot ++ "/" ++ ZnodeName, term_to_binary(Data), ephemeral_sequential),
	{ok, _} = erlzk:exists(Pid, Znode, self()),
	[_, ZnodeSuffix] = string:tokens(Znode, "/"),
	{ok, Children} = erlzk:get_children(Pid, Chroot),
	error_logger:info_msg("Create ephemeral_sequential : ZnodeSuffix : ~p , Children : ~p~n", [Children, ZnodeSuffix]),
	leader_election(State#state{zk_connection = Pid, zk_znode = list_to_binary(Znode), zk_znode_suffix = ZnodeSuffix, 
	zk_revoke_candidates = [], zk_assign_candidates = [], local_resource_list = [], zk_chroot_children = lists:sort(Children)}).


leader_election(#state{zk_znode_suffix = ZnodeSuffix} = State) ->
	leader_election(State#state.zk_chroot_children, ZnodeSuffix, State).

leader_election([ZnodeSuffix|_], ZnodeSuffix, State)->
	error_logger:info_msg("LEADER : ~p~n", [State#state.svr_name]),
	monitor_adjacent_leader(undefined, State#state{role = ?LEADER});
leader_election([AdjLeaderSuffix, ZnodeSuffix|_], ZnodeSuffix, State)->
	error_logger:info_msg("Follwer :[~p] ~n", [State#state.svr_name]),
	monitor_adjacent_leader(AdjLeaderSuffix, State#state{role = ?FOLLOWER});
leader_election([_|AdjLeaderL], ZnodeSuffix, State)->
	leader_election(AdjLeaderL, ZnodeSuffix, State).

monitor_adjacent_leader(AdjLeaderSuffix, #state{role = ?FOLLOWER, zk_chroot = Chroot, zk_connection = Conn}= State) ->
	FQZnodeName = Chroot ++ "/" ++ AdjLeaderSuffix,
	erlzk:exists(Conn, FQZnodeName, self()),
	State#state{zk_adj_leader = list_to_binary(FQZnodeName), zk_chroot_children = [], zk_revoke_candidates = [], zk_assign_candidates = []};
monitor_adjacent_leader(undefined, #state{role = ?LEADER}= State) ->
	State#state{zk_adj_leader = undefined, zk_revoke_candidates = [], zk_assign_candidates = []}.
		

proceed_to_rebalance(ZnodeSuffix, State)->
	ZNodes = State#state.zk_chroot_children,
	PrevRRs =[{I, Z, D} || {Z, [{'group.instance.id', I},{'instance.data', D}]} <- [{Znode0, binary_to_term(Data)}|| {Znode0, {ok, {Data, _}}} 
	              <- [{Znode0, erlzk:get_data(State#state.zk_connection, State#state.zk_chroot ++ "/" ++ Znode0)} 
				    || Znode0 <- ZNodes]]],
	NewRRs = rebalance(State#state.resource_list, PrevRRs),
	RvkCandidates = revoke_candidates(lists:keysort(1,NewRRs), lists:keysort(1,PrevRRs), []),
	AsgCandidates = assign_candidates(lists:keysort(1,NewRRs), lists:keysort(1,PrevRRs), []),
	error_logger:info_msg("NewRRs : ~p~n PrevRRs : ~p~n RvkCandidates : ~p~n AsgCandidates : ~p~n",[NewRRs, PrevRRs, RvkCandidates, AsgCandidates]),
	if 
		RvkCandidates == [] ->
			[erlzk:set_data(State#state.zk_connection, State#state.zk_chroot ++ "/" ++ Z,
				 term_to_binary([{'group.instance.id', I},{'instance.data', D}]), -1)
					|| {I, Z, D} <- AsgCandidates],
			case lists:keymember(ZnodeSuffix, 2, AsgCandidates) of
				true ->
					%%io:fwrite("[~p|~p] {ZnodeSuffix, AsgCandidates} : ~p~n",[?MODULE, ?LINE, {ZnodeSuffix, AsgCandidates}]),
					erlzk:exists(State#state.zk_connection, State#state.zk_znode, self()),
					{noreply, State#state{zk_adj_leader = undefined, role = ?LEADER, zk_revoke_candidates = [], zk_assign_candidates = []}};
				_ ->
					{ok,  Children0} = erlzk:get_children(State#state.zk_connection, State#state.zk_chroot),
					case lists:sort(Children0) of
						ZNodes -> %% no change
							error_logger:info_msg("LEADER : no resource change : ~p~n", [State#state.local_resource_list]),
							erlzk:get_children(State#state.zk_connection, State#state.zk_chroot, self()), %% Add watcher
							erlzk:exists(State#state.zk_connection, State#state.zk_znode, self()),
							{noreply, State#state{zk_adj_leader = undefined, role = ?LEADER, zk_revoke_candidates = [], zk_assign_candidates = []}};
						Children ->
							error_logger:info_msg("LEADER : leader_election ZNodes: ~p :~n New Children: ~p~n", [ZNodes, Children]),
							{noreply,  State#state{zk_chroot_children = Children}, 0}
					end
			end;
		true ->
			[
				begin
					erlzk:set_data(State#state.zk_connection, State#state.zk_chroot ++ "/" ++ Z, 
						term_to_binary([{'group.instance.id', I},{'instance.data', D}]), -1),
					erlzk:get_data(State#state.zk_connection, State#state.zk_chroot ++ "/" ++ Z, self())
				end
			|| {I, Z, D} <- RvkCandidates],
			% erlzk:exists(State#state.zk_connection, State#state.zk_znode, self()),
			{noreply,  State#state{zk_adj_leader = undefined, role = ?LEADER, zk_revoke_candidates = RvkCandidates, zk_assign_candidates = AsgCandidates}}
	end.

%%----------------------------------------------------------------------
%%	Rebalance
%%----------------------------------------------------------------------
rebalance(Resources, PrevRRs) ->
	Ring = hash_ring:make(hash_ring:list_to_nodes(Resources)),
	Candidates = [{I, ZN, [L || {hash_ring_node,L,_,1} <- hash_ring:collect_nodes(I, length(Resources), Ring)]} || {I, ZN, _} <- lists:keysort(1, PrevRRs)],
	RRL = rebalance_round_robbin(Candidates, Resources, []),
	rebalance_sticky(RRL, PrevRRs).
 
rebalance_round_robbin(_Candidates, [], ResultL) ->
	Fun = fun({N, ZN}) -> {N, ZN,lists:concat(proplists:get_all_values({N, ZN},ResultL))} end,
	%%NPrevAsg = [{K2, Z2, V2} || {K2, Z2, V2} <- PrevAsg, lists:member({K2, Z2},[{K1, Z1} || {{K1, Z1}, _} <- ResultL])],
	%%rebalance_sticky(lists:keysort(1,lists:map(Fun,proplists:get_keys(ResultL))), NPrevAsg);
	lists:keysort(1,lists:map(Fun,proplists:get_keys(ResultL)));
rebalance_round_robbin([{_N, _ZN, []}|Candidates], Resources, ResultL) ->
	rebalance_round_robbin(Candidates, Resources, ResultL);
rebalance_round_robbin([{N, ZN, [L|Rest]}|Candidates], Resources, ResultL) ->
	NewCandidates = [{K, Z, [V1 || V1 <- V, V1/=L]} || {K, Z, V} <- Candidates],  %% remove already assinged resource from candidate list from following nodes
	RResources = [L0 || L0 <- Resources, L0 /= L],
	rebalance_round_robbin(NewCandidates ++[{N, ZN, Rest}], RResources, [{{N, ZN}, [L]}|ResultL]).
	
rebalance_sticky(NRRList, [])->
	NRRList;
rebalance_sticky([{K, ZN, NRRs}|NRest], PrevRRList)->
	case lists:keysearch(K, 1, PrevRRList) of
		{value, {K, ZN, PrevRRs}} ->
			RevokeL = PrevRRs -- NRRs,
			NAssignL = NRRs -- PrevRRs,
			NRRList = rebalance_sticky_exchange([{K, ZN, NRRs}|NRest], {K, ZN, PrevRRs}, RevokeL, NAssignL),
			rebalance_sticky(NRRList, PrevRRList--[{K, ZN, PrevRRs}]);
		_ ->
			rebalance_sticky(NRest ++ [{K, ZN, NRRs}], PrevRRList)
	end.

rebalance_sticky_exchange([{K, ZN, NRRs}|NRest], _, [], _) ->
	NRest ++ [{K, ZN, NRRs}];
rebalance_sticky_exchange([{K, ZN, NRRs}|NRest], _, _, []) ->
	NRest ++ [{K, ZN, NRRs}];
rebalance_sticky_exchange([{K, ZN, NRRs}|NRest], {K, ZN, PrevRRs}, [R|RevokeL], [A|NAssignL]) ->
	[{NK, [R]}] = [{K1, LL}||{K1, LL} <- [{K1, [L || L <- NRRs1, L==R]} || {K1, _,NRRs1} <- NRest], LL /=[]],
	{value, {NK, NZK, OldRRs}} = lists:keysearch(NK, 1, NRest),
	NewNRest = lists:keyreplace(NK, 1, NRest, {NK, NZK, (OldRRs -- [R]) ++ [A]}),
	rebalance_sticky_exchange([{K, ZN, (NRRs -- [A]) ++ [R]}|NewNRest], {K, ZN, PrevRRs}, RevokeL, NAssignL).
	
revoke_candidates([], [], RvkList)->
	RvkList;
revoke_candidates([{K, ZN, _}|NRest], [{K, ZN, []}|PRest], RvkList)->
	revoke_candidates(NRest, PRest, RvkList);
revoke_candidates([{K, ZN, NVL}|NRest], [{K, ZN, PVL}|PRest], RvkList)->
	case lists:filter(fun(E) -> lists:member(E, NVL) end, PVL) of
		PVL ->
			revoke_candidates(NRest, PRest, RvkList);
		RvkL ->
			revoke_candidates(NRest, PRest, RvkList++[{K, ZN, RvkL}])
	end.
	
assign_candidates([], [], AsgList)->
	AsgList;
assign_candidates([{K, ZN, NVL}|NRest], [{K, ZN, []}|PRest], AsgList)->
	assign_candidates(NRest, PRest, AsgList++[{K, ZN, NVL}]);
assign_candidates([{K, ZN, NVL}|NRest], [{K, ZN, PVL}|PRest], AsgList)->
	AVL = NVL -- PVL,
	case AVL of
		[] ->
			assign_candidates(NRest, PRest, AsgList);
		_ ->
			assign_candidates(NRest, PRest, AsgList++[{K, ZN, NVL}])
	end.
