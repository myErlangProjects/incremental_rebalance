%%%-------------------------------------------------------------------
%% @doc incremental_rebalance default callback module.
%% @end
%%%-------------------------------------------------------------------

-module(incremental_rebalance_default_callback_1).
-author('Chanaka Fernando <contactchanaka@gmail.com>').
-export([init/1]).
-export([isDataChanged/2]).
-export([updateRole/2]).
-export([onResourceRevoked/2, onResourceAssigned/2]).
-export([rebalance/3]).
-export([revokeCandidates/3, assignCandidates/3]).

-define(LEADER, 'LEADER').		%% Leader and Coordinator
-define(FOLLOWER, 'FOLLOWER').	%% Follower
-define(RESOURCE_LIST,[link1, link2, link3, link4, link5]).
-record(state,{local_resource_list :: undefined | list(),
                resource_list :: undefined | list(),
                instance_id :: undefined | string(),
                role :: undefined | integer()}).

init(InstanceId) ->
    ResourceList = ?RESOURCE_LIST,
    InitialData = [],
    {ok, InitialData, #state{local_resource_list =InitialData, resource_list = ResourceList, instance_id = InstanceId}}.

isDataChanged([{'group.instance.id', _InstanceId},{'instance.data', NData}], #state{local_resource_list = PData, role = ?LEADER} = State) ->
    RvkLinks = PData -- NData,
    AsgLinks = NData -- PData,
    if
        RvkLinks /= [] ->
            error_logger:info_msg("[~p] LEADER Revoke called : Revoke resources : ~p~n", [?MODULE,RvkLinks]),
            {ok, revoke, State};
        true ->
            if
                AsgLinks /=[] ->
                    error_logger:info_msg("[~p] LEADER Assign called : Assign resources : ~p~n", [?MODULE,AsgLinks]),
                    {ok, assign, State};  
                true ->
                    error_logger:info_msg("[~p] LEADER No change resources~n", [?MODULE]),
                    {ok, none, State} 
            end
    end;
isDataChanged([{'group.instance.id', _InstanceId},{'instance.data', NData}], #state{local_resource_list = PData, role = ?FOLLOWER} = State) ->
    RvkLinks = PData -- NData,
	AsgLinks = NData -- PData,
    if
        RvkLinks /= [] ->
            error_logger:info_msg("[~p] FOLLOWER Revoke called : Revoke resources : ~p~n", [?MODULE,RvkLinks]),
            {ok, revoke, State};
        true ->
            if
                AsgLinks /=[] ->
                    error_logger:info_msg("[~p] FOLLOWER Assign called : Assign resources : ~p~n", [?MODULE,AsgLinks]),
                   {ok, assign, State};  
                true ->
                    error_logger:info_msg("[~p] FOLLOWER No change resources~n", [?MODULE]),
                   {ok, none, State} 
            end
    end.

updateRole(?LEADER, #state{instance_id = InstanceId, role = ?LEADER} = State) ->
    error_logger:info_msg("[~p] LEADER no roleUpdate ~n", [InstanceId]),
    {ok, State};
updateRole(?FOLLOWER, #state{instance_id = InstanceId, role = ?FOLLOWER} = State) ->
    error_logger:info_msg("[~p] FOLLOWER no roleUpdate : FOLLOWER~n", [InstanceId]),
    {ok, State};
updateRole(?LEADER, #state{instance_id = InstanceId} = State) ->
    error_logger:info_msg("[~p] roleUpdate : LEADER ~n", [InstanceId]),
    {ok, State#state{role = ?LEADER}};
updateRole(?FOLLOWER, #state{instance_id = InstanceId} = State) ->
    error_logger:info_msg("[~p] FOLLOWER roleUpdate : FOLLOWER~n", [InstanceId]),
    {ok, State#state{role = ?FOLLOWER}}.


onResourceRevoked([{'group.instance.id', InstanceId},{'instance.data', NData}], #state{role = ?LEADER} = State) ->
    error_logger:info_msg("onResourceRevoked : ~p~n", [[{'group.instance.id', InstanceId},{'instance.data', NData}]]),
    error_logger:info_msg("[~p] LEADER RESOURCES : ~p~n", [InstanceId, NData]),
    receive after 100 -> ok end,
    {ok, State#state{local_resource_list = NData}};

onResourceRevoked([{'group.instance.id', InstanceId},{'instance.data', NData}], #state{role = ?FOLLOWER} = State) ->
    error_logger:info_msg("onResourceRevoked : ~p~n", [[{'group.instance.id', InstanceId},{'instance.data', NData}]]),
    error_logger:info_msg("[~p] FOLLOWER RESOURCES : ~p~n", [InstanceId, NData]),
    receive after 100 -> ok end,
    {ok, State#state{local_resource_list = NData}}.

onResourceAssigned([{'group.instance.id', InstanceId},{'instance.data', NData}], #state{role = ?LEADER} = State) ->
    error_logger:info_msg("onResourceAssigned : ~p~n", [[{'group.instance.id', InstanceId},{'instance.data', NData}]]),
    error_logger:info_msg("[~p] LEADER RESOURCES : ~p~n", [InstanceId, NData]),
    {ok, State#state{local_resource_list = NData}};

onResourceAssigned([{'group.instance.id', InstanceId},{'instance.data', NData}], #state{role = ?FOLLOWER} = State) ->
    error_logger:info_msg("onResourceAssigned : ~p~n", [[{'group.instance.id', InstanceId},{'instance.data', NData}]]),
    error_logger:info_msg("[~p] FOLLOWER RESOURCES : ~p~n", [InstanceId, NData]),
    {ok, State#state{local_resource_list = NData}}.

rebalance(Candidates, PrevRRs, State) ->
    Resources = State#state.resource_list,
    error_logger:info_msg("[callback] rebalance : ~p~n", [Resources]),
    RRL = rebalance_round_robbin(Candidates, lists:usort(Resources)),
    {ok, State, rebalance_sticky(RRL, PrevRRs)}.
     
rebalance_round_robbin(Candidates, []) ->
    lists:keysort(1,Candidates);
rebalance_round_robbin([{N, ZN, RRs}|Candidates], [L|RResources]) ->
    rebalance_round_robbin(Candidates ++[{N, ZN, [L|RRs]}], RResources).
   
rebalance_sticky(NRRList, [])->
    NRRList;
rebalance_sticky([{K, ZN, NRRs}|NRest], PrevRRList)->
    case lists:keysearch(K, 1, PrevRRList) of
    {value, {K, ZN, []}} ->
        rebalance_sticky(NRest ++ [{K, ZN, NRRs}], PrevRRList--[{K, ZN, []}]);
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
        
revokeCandidates([], [], RvkList)->
    RvkList;
revokeCandidates([{K, ZN, _}|NRest], [{K, ZN, []}|PRest], RvkList)->
    revokeCandidates(NRest, PRest, RvkList);
revokeCandidates([{K, ZN, NVL}|NRest], [{K, ZN, PVL}|PRest], RvkList)->
    case lists:filter(fun(E) -> lists:member(E, NVL) end, PVL) of
        PVL ->
            revokeCandidates(NRest, PRest, RvkList);
        RvkL ->
            revokeCandidates(NRest, PRest, RvkList++[{K, ZN, RvkL}])
    end.
        
assignCandidates([], [], AsgList)->
    AsgList;
assignCandidates([{K, ZN, NVL}|NRest], [{K, ZN, []}|PRest], AsgList)->
    assignCandidates(NRest, PRest, AsgList++[{K, ZN, NVL}]);
assignCandidates([{K, ZN, NVL}|NRest], [{K, ZN, PVL}|PRest], AsgList)->
    AVL = NVL -- PVL,
    case AVL of
        [] ->
            assignCandidates(NRest, PRest, AsgList);
        _ ->
            assignCandidates(NRest, PRest, AsgList++[{K, ZN, NVL}])
    end.