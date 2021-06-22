-module(client).
-export([start/1,stop/0]).
-export([clientHandlerLoop/3]).
-export([get/0, append/1]).

-define(Dbg(Str),io:format("[DBG]~p:" ++ Str,[?FUNCTION_NAME])).
-define(Dbg(Str,Args),io:format("[DBG]~p:" ++ Str,[?FUNCTION_NAME|Args])).

start(ServerNode) ->
    case net_kernel:connect_node(ServerNode) of
        true ->
            {clientRequest, ServerNode} ! {getNodeList, self()},
            receive
                {ok, NodeList} ->
                    lists:foreach(fun (X) -> 
                        net_kernel:connect_node(X)
                        end, NodeList),

                    register(clientHandler, spawn(?MODULE, clientHandlerLoop,[0, nodes(hidden), []])),
                    io:format("Conectado! ~n");
                _ ->
                    io:format("Error al obtener lista de nodos ~n")
            end;
        _ ->
            io:format("No se pudo establecer conexion con el nodo ~n")
    end.

stop() ->

    lists:foreach(fun (X) -> 
        net_kernel:disconnect(X)
        end, nodes(hidden)),

    io:format("Desconectado! ~n"),

    clientHandler ! fin,
    unregister(clientHandler).

clientHandlerLoop(RequestNumber, NodeList, RequestedList) ->
    receive
        get ->
            lists:foreach(fun (X) -> 
                {clientRequest, X} ! {get, node(), RequestNumber}
                end, NodeList),
            clientHandlerLoop(RequestNumber + 1, NodeList, RequestedList ++ [RequestNumber]);

        {append, Msg} ->
            lists:foreach(fun (X) -> 
                {clientRequest, X} ! {append, Msg, node(), RequestNumber}
                end, NodeList),
            clientHandlerLoop(RequestNumber + 1, NodeList, RequestedList ++ [RequestNumber]);

        {appendres, ReqNumberRes, Msg} ->
            case lists:member(ReqNumberRes, RequestedList) of
                true ->
                    io:format("Se agrego: ~p ~n", [Msg]),
                    clientHandlerLoop(RequestNumber, NodeList, lists:delete(ReqNumberRes, RequestedList));
                false ->
                    clientHandlerLoop(RequestNumber, NodeList, RequestedList)
            end;
        
        {getres, ReqNumberRes, Ledger} ->
            case lists:member(ReqNumberRes, RequestedList) of
                true ->
                    io:format("Ledger: ~p ~n", [Ledger]),
                    clientHandlerLoop(RequestNumber, NodeList, lists:delete(ReqNumberRes, RequestedList));
                false ->
                    clientHandlerLoop(RequestNumber, NodeList, RequestedList)
            end;
        fin ->
            ?Dbg("Handler Closed~n")
    end.


get() ->
    clientHandler ! get.

append(Data) ->
    clientHandler ! {append, Data}.

