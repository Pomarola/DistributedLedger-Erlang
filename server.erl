-module(server).
%-include("struct.hrl").
-export([start/0,stop/0]).
-export([clientRequestsLoop/0, serverRequestsLoop/0]).
-export([sendPropose/3, nodeMonitorLoop/0]).
-export([atomicBroadcast/2]).
-export([appendPendingLoop/1, getPendingLoop/1, ledgerHandler/1, actionQueueHandler/4]).

-define(Dbg(Str),io:format("[DBG]~p:" ++ Str,[?FUNCTION_NAME])).
-define(Dbg(Str,Args),io:format("[DBG]~p:" ++ Str,[?FUNCTION_NAME|Args])).


start() ->
    register(clientRequest,spawn(?MODULE, clientRequestsLoop,[])),
    register(serverRequest,spawn(?MODULE, serverRequestsLoop,[])),
    register(nodeMonitor,spawn(?MODULE, nodeMonitorLoop,[])),

    register(appendPending, spawn(?MODULE, appendPendingLoop,[maps:new()])),
    register(getPending, spawn(?MODULE, getPendingLoop,[sets:new()])),
    register(ledger, spawn(?MODULE, ledgerHandler,[[]])),
    register(actionQueue, spawn(?MODULE, actionQueueHandler,[[], 0, 0, 0])).

stop() ->
    clientRequest ! fin,
    serverRequest ! fin,
    nodeMonitor ! fin,
    appendPending ! fin,
    getPending ! fin,
    ledger ! fin,
    actionQueue ! fin,

    unregister(clientRequest),
    unregister(serverRequest),
    unregister(nodeMonitor),
    unregister(appendPending),
    unregister(getPending),
    unregister(ledger),
    unregister(actionQueue).

clientRequestsLoop() ->
    receive
        {append, Msg, ClientNode, RequestNumber} ->         %receive(c,append,msg) from Client
            spawn(?MODULE, atomicBroadcast, [{append, Msg}, {{ClientNode, RequestNumber}, node()}]),      %Abroadcast
            appendPending ! {add, Msg, {ClientNode, RequestNumber}},        %add to appendPending
            clientRequestsLoop();

        {get, ClientNode, RequestNumber} ->         %receive(c,get) from Client
            spawn(?MODULE, atomicBroadcast, [get, {{ClientNode, RequestNumber}, node()}]),        %Abroadcast
            getPending ! {add, {ClientNode, RequestNumber}},        %add to getPending
            clientRequestsLoop();

        {getNodeList, CliPid} ->
            CliPid ! {ok, nodes()},
            clientRequestsLoop();

        fin ->
            ?Dbg("Client Requests Closed~n")
    end.

serverRequestsLoop() ->
    receive
        {propose, Pid, Id, Type} ->
            {_, Node} = Id,
            ?Dbg("Node: ~p ~n" ,[Node]),
            nodeMonitor ! {startMonitor, Node},
            spawn(?MODULE, sendPropose, [Pid, Type, Id]), 
            serverRequestsLoop();

        {agreed, Id, Number} ->
            {_, Node} = Id,
            nodeMonitor ! {endMonitor, Node},
            actionQueue ! {agreed, Id, Number},
            serverRequestsLoop();

        fin ->
            ?Dbg("Server Requests Closed~n")
    end.

nodeMonitorLoop() ->
    receive
        {startMonitor, Node} ->
            monitor_node(Node, true),
            nodeMonitorLoop();
        {endMonitor, Node} ->
            monitor_node(Node, false),
            nodeMonitorLoop();
        {nodedown, Node} ->
            actionQueue ! {nodedown, Node},
            nodeMonitorLoop();
        fin -> 
            ?Dbg("Node Monitor Closed~n");
        _ -> 
            ?Dbg("Node Monitor Recv cualq~n"),
            nodeMonitorLoop()
    end.

sendPropose(Pid, Type, Id) ->
    actionQueue ! {newProp, self(), Type, Id},
    receive       
        {propose, PropNumber} -> 
            Pid ! {proposed, PropNumber};
        _ ->
            ?Dbg("AQ envio cualq~n")
    end.

atomicBroadcast(Action, Id) ->
    actionQueue ! {newProp, self(), Action, Id},
    receive 
        {propose, PropNumber} -> 
            NodeList = nodes(),
            lists:foreach(fun (X) -> 
                {serverRequest, X} ! {propose, self(), Id, Action},
                monitor_node(X, true)
                end, NodeList),

            Number = responseHandler(length(NodeList), PropNumber),

            lists:foreach(fun (X) -> 
                {serverRequest, X} ! {agreed, Id, Number},
                monitor_node(X, false)
                end, NodeList),
                
            actionQueue ! {agreed, Id, Number};
        _ ->
            ?Dbg("AQ envio cualq en el AB~n")
    end.

responseHandler(0, MaxNumber) ->
    MaxNumber;
responseHandler(Counter, MaxNumber) ->
    receive
        {proposed, PropNumber} ->
            case whosBigger(MaxNumber, PropNumber) of
                first ->
                    responseHandler(Counter-1, MaxNumber);
                second ->
                    responseHandler(Counter-1, PropNumber)
            end;
        _ ->
            ?Dbg("Error Response ~n"),
            responseHandler(Counter-1, MaxNumber)
    end.

actionQueueHandler(Queue, ANumber, PNumber, TO) ->

    receive
        {newProp, Pid, Type, Id} ->
            MaxNumber = max(PNumber, ANumber) + 1,
            Pid ! {propose, {MaxNumber, node()}},
            NewAction = {Type, Id, {MaxNumber, node()}, prop},
            actionQueueHandler(insertSorted(Queue, NewAction), ANumber, MaxNumber, 0);

        {agreed, Id, {AgreedNumber, Node}} ->
            {Type, _, _, _} = lists:keyfind(Id, 2, Queue),
            AgreedAction = {Type, Id, {AgreedNumber, Node}, agreed},
            actionQueueHandler(insertSorted(lists:keydelete(Id, 2, Queue), AgreedAction), AgreedNumber, PNumber, 0);

        {nodedown, Node} ->
            FilteredQueue = lists:filter(fun (X) -> 
                {_, _, {_, XNode}, State} = X,
                not({Node, prop} == {XNode, State})
                end ,Queue),
            actionQueueHandler(FilteredQueue, ANumber, PNumber, 0);

        fin ->
            ?Dbg("Queue Closed~n")

    after TO ->
        case Queue of 
            [] ->
                actionQueueHandler(Queue, ANumber, PNumber, infinity);
            _ ->
                [{Type, Id, _, State}| Qs] = Queue,
                case State of
                    agreed ->
                        case Type of
                            {append, Msg} ->
                                ledger ! {append, Msg, Id};
                            get ->
                                ledger ! {get, Id}
                        end,
                        actionQueueHandler(Qs, ANumber, PNumber, 0);
                    
                    prop ->
                        actionQueueHandler(Queue, ANumber, PNumber, infinity)
                end
        end
    end.

insertSorted([], NewAction) ->
    [NewAction];
insertSorted([Action | Qs], NewAction) ->
    {_, _, Number1, _} = Action,
    {_, _, Number2, _} = NewAction,
    case whosBigger(Number1, Number2) of
        first ->
            [NewAction] ++ [Action | Qs];
        second ->
            [Action] ++ insertSorted(Qs, NewAction)
    end.

whosBigger({Prop1, Node1}, {Prop2, Node2}) ->
    if 
        Prop1 > Prop2 -> first;
        Prop1 == Prop2 ->
            if
                Node1 > Node2 ->
                    first;
                true ->
                    second
            end;
        true -> second
    end.

appendPendingLoop(AppendMap) ->
    receive
        {add, Msg, Id} ->
            appendPendingLoop(maps:put(Id, Msg, AppendMap));
        {appendExecuted, Id} ->
            case maps:find(Id, AppendMap) of        %find if Id belongs to appendPending
                {ok, Msg} ->
                    {ClientNode, RequestNumber} = Id,
                    {clientHandler, ClientNode} ! {appendres, RequestNumber, Msg},           %send append ack to client
                    appendPendingLoop(maps:remove(Id, AppendMap));          %remove from appendPending
                error -> 
                    appendPendingLoop(AppendMap)
            end;
        fin ->
            ?Dbg("Pending Append Closed~n")
    end.

getPendingLoop(PendingSet) ->
    receive
        {add, Id} ->
            getPendingLoop(sets:add_element(Id, PendingSet));
        {getExecuted, Ledger, Id} ->
            case sets:is_element(Id, PendingSet) of         %find if Id belongs to getPending
                true ->
                    {ClientNode, RequestNumber} = Id,
                    {clientHandler, ClientNode} ! {getres, RequestNumber, Ledger},      %send ledger to client
                    getPendingLoop(sets:del_element(Id, PendingSet));   %remove from getpending
                false ->
                    getPendingLoop(PendingSet)
            end;
        fin ->
            ?Dbg("Pending Get Closed~n")
    end.

ledgerHandler(Ledger) ->
    receive
        {get, {Id, _}} ->      % get deliver
            getPending ! {getExecuted, Ledger, Id},
            ledgerHandler(Ledger);
        {append, MsgAgreed, {Id, _}} ->     % append deliver
            case lists:keyfind(Id, 2, Ledger) of     %find if its appended or not
                false ->
                    UpdatedLedger = Ledger ++ [{MsgAgreed, Id}],       %if not in ledger, append
                    appendPending ! {appendExecuted, Id},
                    ledgerHandler(UpdatedLedger);
                _ ->
                    ledgerHandler(Ledger)
            end;
        fin ->
            ?Dbg("Ledger Closed~n")
    end.
