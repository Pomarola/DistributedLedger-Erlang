-module(sec).
-include("struct.hrl").
-export([start/0, stop/0]).
-export([secLoop/1]).

start() ->
    register(sequencer, spawn(?MODULE, secLoop,[1])).

stop() ->
    sequencer ! fin,
    unregister(sequencer).

secLoop(SecNum) ->
    receive
        #send{msg = Msg, sender = Sender} ->
            M = #msg{msg = Msg, sender = Sender, sn = SecNum},
            lists:foreach(fun (X) -> {dest, X} ! M end, nodes()),
            secLoop(SecNum + 1);
        fin -> ok
    end.
