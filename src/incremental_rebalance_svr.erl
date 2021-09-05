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
-record(state, {zk_chroot :: undefined | string(),
				zk_connection :: undefined | pid(),
				zk_svr_list :: undefined | list(),
				zk_znode :: undefined | binary(),
				zk_chroot_children :: undefined | list(),
				zk_znode_suffix :: undefined | list(),
				zk_adj_leader :: undefined | binary(),
				zk_revoke_candidates :: undefined | list(),
				zk_assign_candidates :: undefined | list(),
				instance_id :: undefined | string(),
				callback :: undefined | atom(),
				callback_state :: undefined | term(),
				role :: undefined | integer()
			}).

%%----------------------------------------------------------------------
%%  The incremental_rebalance_svr Macros
%%----------------------------------------------------------------------
-define(LEADER, 'LEADER').		%% Leader and Coordinator
-define(FOLLOWER, 'FOLLOWER').	%% Follower
-define(CONN_WAIT_T_MS, 1000).
-define(RECON_WAIT_T_MS, 1000).
-define(REPEAT_REBAL_T_MS, 10000).
-define(REBAL_REVOKE_TIMEOUT_T_MS, 20000).
-define(INIT_REBAL_T_MS, application:get_env(incremental_rebalance, 'scheduled.rebalance.max.delay.ms', 20000)).
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
init([CallbackModule]) ->
	process_flag(trap_exit, true),
	ZkHostPortList = application:get_env(incremental_rebalance, 'zk.server.list',"127.0.0.1:2181"),
	ZkSvrList = [{H, list_to_integer(P)}|| [H,P] <- [string:tokens(ZkHostPort, ": ") || ZkHostPort <- string:tokens(ZkHostPortList, ", ")]],
	ChrootPrefix = application:get_env(incremental_rebalance, 'zk.chroot.prefix',"zk"),
	Chroot	= "/" ++ ChrootPrefix ++ "-" ++ atom_to_list(CallbackModule),
	{ok, Pid} = erlzk:connect(ZkSvrList, 30000,[{monitor, self()}]),
	link(Pid),
	{Mega,Sec,Milli} = os:timestamp(),
	AutoInstantId = lists:concat([integer_to_list(N) || N <- [Mega,Sec,Milli]]),
	InstanceId = application:get_env(incremental_rebalance, 'group.instance.id', AutoInstantId),
	error_logger:info_msg("Starting : ~p ~n", [InstanceId]),
	{ok, #state{callback = CallbackModule, zk_connection = Pid, zk_chroot = Chroot, zk_svr_list = ZkSvrList, instance_id = InstanceId}}.											
	

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
handle_info(timeout, 
		#state{zk_connection = Pid,zk_znode = Znode,zk_chroot = Chroot, 
			zk_znode_suffix = ZnodeSuffix, instance_id = InstanceId, role = ?LEADER} = State) ->
	error_logger:info_msg("[~p] LEADER : proceed_to_rebalance : ~p ~n", [InstanceId, Znode]),
	{ok, Children} = erlzk:get_children(Pid, Chroot),  %% Remove watcher, Get latest node list
	NewState = State#state{zk_chroot_children = lists:sort(Children)},
	proceed_to_rebalance(ZnodeSuffix, NewState);
handle_info({node_children_changed, ChrootBin}, 
		#state{zk_connection = Pid,zk_znode = Znode,zk_chroot = Chroot, instance_id = InstanceId, role = ?LEADER} = State) ->
	error_logger:info_msg("[~p] LEADER : ~p : ({node_children_changed, Chroot}  : ~p ~n", [InstanceId, Znode, {node_children_changed, ChrootBin}]),
	{ok, Children} = erlzk:get_children(Pid, Chroot),  %% Remove watcher
	NewState = State#state{zk_chroot_children = lists:sort(Children)},
	{noreply, NewState, ?INIT_REBAL_T_MS};
handle_info({node_deleted, DelZbode}, 
	#state{zk_connection = Pid,zk_chroot = Chroot, 
		zk_znode = Znode, instance_id = InstanceId, role = ?LEADER} = State) ->
	error_logger:info_msg("[~p] LEADER : ~p : {node_deleted,DelZbode} : ~p ~n", [InstanceId, Znode, {node_deleted,DelZbode}]),
	{ok, Children} = erlzk:get_children(Pid, Chroot),  %% Remove watcher
	NewState = State#state{zk_chroot_children = lists:sort(Children)},
	{noreply, NewState, ?INIT_REBAL_T_MS};
handle_info({node_data_changed, Znode}, 
		#state{zk_chroot = Chroot, zk_znode = Znode, zk_chroot_children = ChildZNodes, zk_connection = Pid,
			 zk_revoke_candidates = [], zk_assign_candidates = [], callback_state = CallbackState, instance_id = InstanceId, role = ?LEADER} = State) ->
	{ok, {Data, _}} = erlzk:get_data(Pid, Znode),
	error_logger:info_msg("LEADER : zk_revoke_candidates : ~p~n zk_assign_candidates : ~p~n", [[],[]]),
	DecodedData = binary_to_term(Data),
	{ok, Action, DataChangedCallbackState} = (State#state.callback):isDataChanged(DecodedData, CallbackState),
	case Action of
		revoke ->
			%% Revoke internal process
			{ok, NewCallbackState} = (State#state.callback):onResourceRevoked(DecodedData, DataChangedCallbackState),
			Status = erlzk:set_data(Pid, binary_to_list(Znode), Data, -1),	%% redundant set_data to inform leader that revoke is done.
			error_logger:info_msg("Znode : ~p set_data : ~p~n",[Znode, Status]);
		assign ->
			%% Assign internal process
		    {ok, NewCallbackState} = (State#state.callback):onResourceAssigned(DecodedData, DataChangedCallbackState);
		_ ->
			{ok, NewCallbackState} = {ok, DataChangedCallbackState}
	end,
	erlzk:get_data(Pid, Znode, self()),
	{ok, Children0} = erlzk:get_children(Pid, Chroot),
	case lists:sort(Children0) of
		ChildZNodes ->
			erlzk:get_children(Pid, Chroot, self()), %% Add watcher
			NewState = State#state{callback_state = NewCallbackState},
			{noreply, NewState};
		Children -> %% children changed
			error_logger:info_msg("[~p] LEADER :[1] REPEAT proceed_to_rebalance : ~p~n", [InstanceId, Children]),
			NewState = State#state{zk_revoke_candidates = [], zk_assign_candidates = [], callback_state = NewCallbackState, zk_chroot_children = Children},
			{noreply, NewState, ?REPEAT_REBAL_T_MS}
	end;
handle_info({node_data_changed, Znode}, 
		#state{zk_chroot = Chroot, zk_znode = Znode, zk_znode_suffix = ZnodeSuffix,
			zk_chroot_children = ChildZNodes,zk_connection = Pid, zk_revoke_candidates = RvkCandidates, 
				zk_assign_candidates = AsgCandidates, callback_state = CallbackState, instance_id = InstanceId, role = ?LEADER} = State) ->
	{ok, {Data, _}} = erlzk:get_data(Pid, Znode),
	error_logger:info_msg("LEADER : zk_revoke_candidates : ~p~n zk_assign_candidates : ~p~n", [RvkCandidates,AsgCandidates]),
	DecodedData = binary_to_term(Data),
	{ok, Action, DataChangedCallbackState} = (State#state.callback):isDataChanged(DecodedData, CallbackState),
	case Action of
		revoke ->
			%% Revoke internal process
			{ok, NewCallbackState} = (State#state.callback):onResourceRevoked(DecodedData, DataChangedCallbackState),
			erlzk:set_data(Pid, binary_to_list(Znode), Data, -1);	%% redundant set_data to inform leader that revoke is done.
		assign ->
			%% Assign internal process
		    {ok, NewCallbackState} = (State#state.callback):onResourceAssigned(DecodedData, DataChangedCallbackState);
		_ ->
			{ok, NewCallbackState} = {ok, DataChangedCallbackState}
	end,
	erlzk:get_data(Pid, Znode, self()),
	NewRvkCandidates = lists:keydelete(ZnodeSuffix, 2, RvkCandidates),
	if 
		NewRvkCandidates == [] ->
			[erlzk:set_data(Pid, Chroot ++ "/" ++ Z, term_to_binary([{'group.instance.id', I},{'instance.data', D}]), -1)
			|| {I, Z, D} <- AsgCandidates],
			case lists:keymember(ZnodeSuffix, 2, AsgCandidates) of
				true ->
					NewState = State#state{zk_revoke_candidates = [], zk_assign_candidates = [], callback_state = NewCallbackState},
					{noreply, NewState};
				_ ->
					{ok, Children0} = erlzk:get_children(State#state.zk_connection, State#state.zk_chroot),
					case  lists:sort(Children0) of
						ChildZNodes ->
							error_logger:info_msg("LEADER : no resource change ~n", []),
							erlzk:get_children(State#state.zk_connection, State#state.zk_chroot, self()), %% add watcher
							NewState = State#state{zk_revoke_candidates = [], zk_assign_candidates = [], callback_state = NewCallbackState},
							{noreply, NewState};
						Children ->
							error_logger:info_msg("[~p] LEADER : REPEAT proceed_to_rebalance  : ~p ~n", [InstanceId, Children]),
							NewState = State#state{zk_revoke_candidates = [], zk_assign_candidates = [], callback_state = NewCallbackState, zk_chroot_children = Children},
							{noreply, NewState, ?REPEAT_REBAL_T_MS}
					end
			end;
		true ->
			NewState = State#state{zk_revoke_candidates = NewRvkCandidates, zk_assign_candidates = AsgCandidates, 
						callback_state = NewCallbackState},
			{noreply, NewState, ?REBAL_REVOKE_TIMEOUT_T_MS}
	end;
handle_info({node_data_changed, FZnode}, 
	#state{zk_chroot = Chroot, zk_znode = Znode, zk_znode_suffix = ZnodeSuffix,
		zk_chroot_children = ChildZNodes,zk_connection = Pid, zk_revoke_candidates = RvkCandidates, 
			zk_assign_candidates = AsgCandidates, instance_id = InstanceId, role = ?LEADER} = State) ->
	[_, FZnodeSuffix] = string:tokens(binary_to_list(FZnode), "/"),
	NewRvkCandidates = lists:keydelete(FZnodeSuffix, 2, RvkCandidates),
	error_logger:info_msg("[~p] LEADER :[3] ~p : {node_data_changed, FZnode} : ~p ~n Data :~p~n", 
		[InstanceId, Znode, {node_data_changed, FZnode}]),
	error_logger:info_msg("LEADER : zk_revoke_candidates : ~p~n zk_assign_candidates : ~p~n", [NewRvkCandidates,AsgCandidates]),
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
							error_logger:info_msg("LEADER : no resource change ~n", []),
							erlzk:get_children(State#state.zk_connection, State#state.zk_chroot, self()), %% add watcher
							NewState = State#state{zk_revoke_candidates = [], zk_assign_candidates = []},
							{noreply, NewState};
						Children ->
							error_logger:info_msg("[~p] LEADER : REPEAT proceed_to_rebalance  : ~p ~n", [InstanceId, Children]),
							NewState = State#state{zk_revoke_candidates = [], zk_assign_candidates = [], zk_chroot_children = Children},
							{noreply, NewState, ?REPEAT_REBAL_T_MS}
					end
			end;
		true ->
			NewState = State#state{zk_revoke_candidates = NewRvkCandidates, zk_assign_candidates = AsgCandidates},
			{noreply, NewState, ?REBAL_REVOKE_TIMEOUT_T_MS}
	end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%% FOLLOWER messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_info(timeout, #state{role = ?FOLLOWER, zk_znode_suffix = ZnodeSuffix} = State) ->
	error_logger:info_msg("FOLLOWER : ignore proceed_to_rebalance : ~p ~n", [ZnodeSuffix]),
	{noreply, State};
handle_info({node_deleted,AdjLeader}, 
		#state{zk_connection = Pid,zk_chroot = Chroot, callback_state = CallbackState,
			zk_znode = Znode, zk_adj_leader = AdjLeader, instance_id = InstanceId, role = ?FOLLOWER} = State) ->
	error_logger:info_msg("[~p] FOLLOWER : ~p : {node_deleted,AdjLeader} : ~p ~n", [InstanceId, Znode, {node_deleted,AdjLeader}]),
	{ok, Children} = erlzk:get_children(Pid, Chroot),
	NewState = leader_election(State#state{zk_chroot_children = lists:sort(Children)}),
	{ok, NewCallbackState} = (State#state.callback):updateRole(NewState#state.role, CallbackState),
	{noreply, NewState#state{callback_state = NewCallbackState}, ?INIT_REBAL_T_MS};
handle_info({node_data_changed, Znode}, 
	#state{zk_znode = Znode,zk_connection = Pid, callback_state = CallbackState, role = ?FOLLOWER} = State) ->
	{ok, {Data, _}} = erlzk:get_data(Pid, Znode),
	DecodedData = binary_to_term(Data),
	{ok, Action, DataChangedCallbackState} = (State#state.callback):isDataChanged(DecodedData, CallbackState),
	case Action of
		revoke ->
			%% Revoke internal process
			{ok, NewCallbackState} = (State#state.callback):onResourceRevoked(DecodedData, DataChangedCallbackState),
			Status = erlzk:set_data(Pid, binary_to_list(Znode), Data, -1),	%% redundant set_data to inform leader that revoke is done.
			error_logger:info_msg("Znode : ~p set_data : ~p~n",[Znode, Status]);
		assign ->
			%% Assign internal process
		    {ok, NewCallbackState} = (State#state.callback):onResourceAssigned(DecodedData, DataChangedCallbackState);
		_ ->
			{ok, NewCallbackState} = {ok, DataChangedCallbackState}
	end,
	erlzk:get_data(Pid, Znode, self()),
	{noreply, State#state{callback_state = NewCallbackState}};
handle_info({node_data_changed, AdjLeader}, #state{zk_adj_leader = AdjLeader, zk_connection = Pid, role = ?FOLLOWER} = State) ->
	erlzk:exists(Pid, AdjLeader, self()),
	{noreply, State};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%	
%% COMMON messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
handle_info({disconnected, Host, Port}, #state{instance_id = InstanceId} = State) ->
	error_logger:info_msg("ZK session disconnected : ~p|~p : Info : ~p~n", [InstanceId, Host, Port]),
	{noreply, State};
handle_info({expired, Host, Port}, #state{instance_id = InstanceId} = State) ->
	error_logger:info_msg("ZK session expired : ~p|~p : Info : ~p~n", [InstanceId, Host, Port]),
	{noreply, State};
handle_info({connected, Host, Port}, #state{zk_znode = undefined, instance_id = InstanceId} = State) ->
	error_logger:info_msg("Initial ZK session connected : ~p|~p : Info : ~p~n", [InstanceId, Host, Port]),
	NewState = initiate_session(State),
	{noreply, NewState, ?CONN_WAIT_T_MS};
handle_info({connected, Host, Port}, #state{ zk_connection = Pid, instance_id = InstanceId, zk_znode = Znode} = State) ->
	error_logger:info_msg("ZK session re-connected : ~p|~p : Info : ~p~n", [InstanceId, Host, Port]),
	erlzk:delete(Pid, Znode),
	NewState = initiate_session(State),
	{noreply, NewState, ?RECON_WAIT_T_MS};
handle_info({node_deleted, Znode}, #state{instance_id = InstanceId, zk_znode = Znode} = State) ->
	error_logger:info_msg("Node deleted : ~p|~p : {node_deleted,Znode} : ~p ~n", [InstanceId, Znode, {node_deleted,Znode}]),
	NewState = initiate_session(State),
	{noreply, NewState, ?RECON_WAIT_T_MS};
handle_info({'EXIT', OldPid, Reason}, #state{zk_svr_list = ZkSvrList, instance_id = InstanceId, zk_znode = Znode} = State) when is_pid(OldPid) ->
	error_logger:error_msg("ZK Conn proc down: ~p|~p : Info : ~p~n", [InstanceId, Znode, {'EXIT', OldPid, Reason}]),
	{ok, Pid} = erlzk:connect(ZkSvrList, 30000,[{monitor, self()}]),
	link(Pid),
	{noreply, State#state{zk_connection = Pid}};

handle_info(Info, #state{zk_znode = Znode, instance_id = InstanceId, zk_connection = Pid} = State) ->
	error_logger:warning_msg("UNKNOWN msg received : ~p|~p : Info : ~p~n", [InstanceId, Znode, Info]),
	erlzk:exists(Pid, Znode, self()),
	{noreply, State}.

-spec terminate(Reason :: normal | shutdown | {shutdown, term()} | term(),
		State::#state{}) ->
	any().
%% @doc Cleanup and exit.
%% @see //stdlib/gen_server:terminate/3
%% @private
%%
terminate(Reason, #state{zk_connection = Pid, instance_id = InstanceId, zk_znode = Znode}=State) ->
	erlzk:delete(Pid, Znode),
	erlzk:close(Pid),
	error_logger:warning_msg("Terminating : ~p | Reason : ~p | State : ~p~n", [InstanceId, Reason, State]),
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
initiate_session(#state{zk_connection = Pid, zk_chroot = Chroot, instance_id = InstanceId, callback = Callback} = State) ->
	ZnodeName = application:get_env(incremental_rebalance, 'zk.znode',"resource"),
	erlzk:create(Pid, Chroot, persistent),
	error_logger:info_msg("Chroot : ~p~n" ,[Chroot]),
	{ok, InitialData, CallbackState} = (Callback):init(InstanceId),
	InitialZnodeData = [{'group.instance.id', InstanceId},{'instance.data', InitialData}],
	{ok, Znode} = erlzk:create(Pid, Chroot ++ "/" ++ ZnodeName, term_to_binary(InitialZnodeData), ephemeral_sequential),
	{ok, _} = erlzk:exists(Pid, Znode, self()),
	[_, ZnodeSuffix] = string:tokens(Znode, "/"),
	{ok, Children} = erlzk:get_children(Pid, Chroot),
	error_logger:info_msg("Create ephemeral_sequential : ZnodeSuffix : ~p , Children : ~p~n", [ZnodeSuffix, Children]),
	NewState = leader_election(State#state{zk_connection = Pid, zk_znode = list_to_binary(Znode), zk_znode_suffix = ZnodeSuffix,
	zk_revoke_candidates = [], zk_assign_candidates = [], callback_state = CallbackState, zk_chroot_children = lists:sort(Children)}),
	{ok, NewCallbackState} = (State#state.callback):updateRole(NewState#state.role, CallbackState),
	NewState#state{callback_state = NewCallbackState}.


leader_election(#state{zk_znode_suffix = ZnodeSuffix} = State) ->
	leader_election(State#state.zk_chroot_children, ZnodeSuffix, State).

leader_election([ZnodeSuffix|_], ZnodeSuffix, State)->
	monitor_adjacent_leader(undefined, State#state{role = ?LEADER});
leader_election([AdjLeaderSuffix, ZnodeSuffix|_], ZnodeSuffix, State)->
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
	FiltPrevRRs = lists:ukeysort(1,lists:reverse(lists:keysort(2,PrevRRs))), %% Remove dead Znodes by removing old ones with duplicate ins_ids
	{ok, NewState, NewRRs} = rebalance(State, FiltPrevRRs),
	RvkCandidates = (NewState#state.callback):revokeCandidates(lists:keysort(1,NewRRs), lists:keysort(1,FiltPrevRRs), []),
	AsgCandidates = (NewState#state.callback):assignCandidates(lists:keysort(1,NewRRs), lists:keysort(1,FiltPrevRRs), []),
	error_logger:info_msg("NewRRs : ~p~n PrevRRs : ~p~n RvkCandidates : ~p~n AsgCandidates : ~p~n",[NewRRs, FiltPrevRRs, RvkCandidates, AsgCandidates]),
	if 
		RvkCandidates == [] ->
			[erlzk:set_data(NewState#state.zk_connection, NewState#state.zk_chroot ++ "/" ++ Z,
				 term_to_binary([{'group.instance.id', I},{'instance.data', D}]), -1)
					|| {I, Z, D} <- AsgCandidates],
			case lists:keymember(ZnodeSuffix, 2, AsgCandidates) of
				true ->
					%%io:fwrite("[~p|~p] {ZnodeSuffix, AsgCandidates} : ~p~n",[?MODULE, ?LINE, {ZnodeSuffix, AsgCandidates}]),
					erlzk:exists(NewState#state.zk_connection, NewState#state.zk_znode, self()),
					{noreply, NewState#state{zk_adj_leader = undefined, role = ?LEADER, zk_revoke_candidates = [], zk_assign_candidates = []}};
				_ ->
					{ok,  Children0} = erlzk:get_children(NewState#state.zk_connection, NewState#state.zk_chroot),
					case lists:sort(Children0) of
						ZNodes -> %% no change
							error_logger:info_msg("LEADER : no resource change ~n", []),
							erlzk:get_children(NewState#state.zk_connection, NewState#state.zk_chroot, self()), %% Add watcher
							erlzk:exists(NewState#state.zk_connection, NewState#state.zk_znode, self()),
							{noreply, NewState#state{zk_adj_leader = undefined, role = ?LEADER, zk_revoke_candidates = [], zk_assign_candidates = []}};
						Children ->
							error_logger:info_msg("LEADER : REPEAT proceed_to_rebalance ZNodes: ~p :~n New Children: ~p~n", [ZNodes, Children]),
							{noreply,  NewState#state{zk_chroot_children = Children}, ?REPEAT_REBAL_T_MS}
					end
			end;
		true ->
			[
				begin
					erlzk:set_data(NewState#state.zk_connection, NewState#state.zk_chroot ++ "/" ++ Z, 
						term_to_binary([{'group.instance.id', I},{'instance.data', D}]), -1),
					erlzk:get_data(NewState#state.zk_connection, NewState#state.zk_chroot ++ "/" ++ Z, self())
				end
			|| {I, Z, D} <- RvkCandidates],
			% erlzk:exists(State#state.zk_connection, State#state.zk_znode, self()),
			{noreply,  NewState#state{zk_adj_leader = undefined, role = ?LEADER, zk_revoke_candidates = RvkCandidates, 
				zk_assign_candidates = AsgCandidates}, ?REBAL_REVOKE_TIMEOUT_T_MS}
	end.

%%----------------------------------------------------------------------
%%	Rebalance
%%----------------------------------------------------------------------

rebalance(State, PrevRRs) ->
	Candidates = [{I, ZN, []} || {I, ZN, _} <- lists:keysort(1, PrevRRs)],
	{ok, NewCallbackState, NewRRs} = (State#state.callback):rebalance(Candidates, PrevRRs, State#state.callback_state),
	{ok, State#state{callback_state = NewCallbackState}, NewRRs} .
