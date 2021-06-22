%% Send information msg + sender
-record(msgPropose, {msg, id}).

%% Msg info Msg x Sender x Seq Num
-record(msgAgree, {msg, id, number}).

-record(actionInQueue, {action, id, order, state}).
