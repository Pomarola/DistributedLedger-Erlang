-module(dest).
-include("struct.hrl").
-export([start/1,stop/0]).
-export([destLoop/3, senderLoop/1, deliver/0]).
-export([send/1]).

start(Seq) ->
    register(dest, spawn(?MODULE, destLoop,[1, dict:new(), 0])),
    register(sender, spawn(?MODULE, senderLoop,[Seq])),
    register(deliver, spawn(?MODULE, deliver,[])).

stop() ->
    dest ! fin,
    deliver ! fin,
    unregister(deliver),
    unregister(dest).

send(Msg) ->
    sender ! #send{msg = Msg, sender = self()},
    ok.

senderLoop(Seq) ->
    receive
        M when is_record(M, send) ->
            {sequencer, Seq} ! M,
            senderLoop(Seq);
        _ ->
            io:format("Recv cualca ~n")
    end.

deliver() ->
    receive
        fin -> exit(normal);
        M ->
            io:format("Deliver : ~p ~n", [M]),
            deliver()
    end.


destLoop(NDel, Pend, TO) ->
    receive
        Data when is_record(Data,msg) ->
            destLoop(NDel
                    , dict:append(Data#msg.sn,#send{msg = Data#msg.msg, sender = Data#msg.sender}, Pend)
                    , 0)
    after TO ->
            case dict:find(NDel, Pend) of
                {ok, Msg} ->
                    deliver ! Msg,
                    destLoop(NDel + 1, Pend, 0);
                error ->
                    destLoop(NDel, Pend, infinity)
            end
    end.
