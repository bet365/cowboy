%% Copyright (c) 2011-2014, Loïc Hoguin <essen@ninenines.eu>
%%
%% Permission to use, copy, modify, and/or distribute this software for any
%% purpose with or without fee is hereby granted, provided that the above
%% copyright notice and this permission notice appear in all copies.
%%
%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

%% Cowboy supports versions 7 through 17 of the Websocket drafts.
%% It also supports RFC6455, the proposed standard for Websocket.
-module(cowboy_websocket).
-behaviour(cowboy_sub_protocol).

-export([upgrade/4]).
-export([handler_loop/4]).

-type terminate_reason() :: normal | stop | timeout
| remote | {remote, cow_ws:close_code(), binary()}
| {error, badencoding | badframe | closed | atom()}.

-record(extensions, {
	deflate = undefined :: undefined | port(),
	deflate_window_bits :: undefined | integer(),
	deflate_takeover    :: undefined | no_takeover | takeover,
	inflate = undefined :: undefined | port(),
	inflate_window_bits :: undefined | integer(),
	inflate_takeover    :: undefined | no_takeover | takeover,
	compression_threshold = 0 :: integer()
}).

-record(extensionOpts, {
	client_takeover = negotiable :: takeover | no_takeover | negotiable,
	client_max_window = 15 :: integer(),
	server_takeover = negotiable :: takeover | no_takeover | negotiable,
	server_max_window = 15 :: integer(),
	compression_threshold = 0 :: integer()
}).

-record(state, {
	env :: cowboy_middleware:env(),
	socket = undefined :: inet:socket(),
	transport = undefined :: module(),
	handler :: module(),
	key = undefined :: undefined | binary(),
	timeout = infinity :: timeout(),
	timeout_ref = undefined :: undefined | reference(),
	messages = undefined :: undefined | {atom(), atom(), atom()},
	hibernate = false :: boolean(),
	frag_state = undefined :: cow_ws:frag_state(),
	frag_buffer = <<>> :: binary(),
	utf8_state = 0 :: cow_ws:utf8_state(),
    extensions = #extensions{} :: #extensions{} | undefined
}).

-spec upgrade(Req, Env, module(), any()) -> {ok, Req, Env} | {suspend, module(), atom(), [any()]}
	when Req::cowboy_req:req(), Env::cowboy_middleware:env().
upgrade(Req, Env, Handler, HandlerOpts) ->
	{_, Ref} = lists:keyfind(listener, 1, Env),
	ranch:remove_connection(Ref),
	[Socket, Transport] = cowboy_req:get([socket, transport], Req),
	State = #state{env=Env, socket=Socket, transport=Transport,
		handler=Handler},
	try websocket_upgrade(State, Req) of
		{ok, State2, Req2} ->
			handler_init(State2, Req2, HandlerOpts)
	catch _:_ ->
		receive
			{cowboy_req, resp_sent} -> ok
		after 0 ->
			_ = cowboy_req:reply(400, Req),
			exit(normal)
		end
	end.

-spec websocket_upgrade(#state{}, Req)
	-> {ok, #state{}, Req} when Req::cowboy_req:req().
websocket_upgrade(State, Req) ->
	{ok, ConnTokens, Req2}
		= cowboy_req:parse_header(<<"connection">>, Req),
	true = lists:member(<<"upgrade">>, ConnTokens),
	%% @todo Should probably send a 426 if the Upgrade header is missing.
	{ok, [<<"websocket">>], Req3}
		= cowboy_req:parse_header(<<"upgrade">>, Req2),
	{Version, Req4} = cowboy_req:header(<<"sec-websocket-version">>, Req3),
	IntVersion = list_to_integer(binary_to_list(Version)),
	true = (IntVersion =:= 7) orelse (IntVersion =:= 8)
		orelse (IntVersion =:= 13),
	{Key, Req5} = cowboy_req:header(<<"sec-websocket-key">>, Req4),
	false = Key =:= undefined,
	websocket_extensions(State#state{key=Key},
		cowboy_req:set_meta(websocket_version, IntVersion, Req5)).

-spec websocket_extensions(#state{}, Req) -> {ok, #state{}, Req} when Req::cowboy_req:req().
websocket_extensions(State, Req) ->
    %% @todo We want different options for this. For example
    %% * compress everything auto
    %% * compress only text auto
    %% * compress only binary auto
    %% * compress nothing auto (but still enabled it)
    %% * disable compression
    case cowboy_req:parse_header(<<"sec-websocket-extensions">>, Req) of
        {ok, Extensions, Req2} when Extensions =/= undefined ->
            [Compress] = cowboy_req:get([resp_compress], Req),
            case Compress of
                true ->
                    websocket_extensions(State, Req2, Extensions, []);
                _ ->
                    {ok, State#state{extensions=undefined}, Req2}
            end;
        _ ->
            {ok, State#state{extensions=undefined}, Req}
    end.

-spec websocket_extensions(#state{}, Req, list(), list()) ->
	{ok, #state{}, Req} when Req :: cowboy_req:req().
websocket_extensions(State, Req, [], []) ->
    {ok, State, Req};
websocket_extensions(State, Req, [], [<<", ">>|RespHeader]) ->
    {ok, State, cowboy_req:set_resp_header(
		<<"sec-websocket-extensions">>, lists:reverse(RespHeader), Req)};
websocket_extensions(State=#state{extensions=Extensions, env=Env}, Req,
    [{<<"permessage-deflate">>, Params}|Tail], RespHeader) ->

	WebSocketOpts = proplists:get_value(websocket_opts, Env, []),
	ClientTakeOver = proplists:get_value(websocket_client_takeover, WebSocketOpts, negotiable),
	ServerTakeOver = proplists:get_value(websocket_server_takeover, WebSocketOpts, negotiable),
	ClientMaxWindow = proplists:get_value(websocket_client_window, WebSocketOpts, 15),
	ServerMaxWindow = proplists:get_value(websocket_server_window, WebSocketOpts, 15),
    CompressionThreshold = cowboy_req:get(resp_compression_threshold, Req),

	ExtensionOpts = #extensionOpts{
		client_takeover = ClientTakeOver,
		client_max_window = ClientMaxWindow,
		server_takeover = ServerTakeOver,
		server_max_window = ServerMaxWindow,
		compression_threshold = CompressionThreshold
	},

    case cow_ws:negotiate_permessage_deflate(Params, Extensions, ExtensionOpts) of
        {ok, RespExt, Extensions2} ->
            websocket_extensions(State#state{
                extensions=Extensions2},
                Req, Tail, [<<", ">>, RespExt|RespHeader]);
        ignore ->
            websocket_extensions(State, Req, Tail, RespHeader)
    end;
websocket_extensions(State=#state{extensions=Extensions}, Req,
    [{<<"x-webkit-deflate-frame">>, Params}|Tail], RespHeader) ->
    %% @todo Make deflate options configurable.
    CompressionThreshold = cowboy_req:get(resp_compression_threshold, Req),
    case cow_ws:negotiate_x_webkit_deflate_frame(Params, Extensions, CompressionThreshold) of
        {ok, RespExt, Extensions2} ->
            websocket_extensions(State#state{
                extensions=Extensions2},
                Req, Tail, [<<", ">>, RespExt|RespHeader]);
        ignore ->
            websocket_extensions(State, Req, Tail, RespHeader)
    end;
websocket_extensions(State, Req, [_|Tail], RespHeader) ->
    websocket_extensions(State, Req, Tail, RespHeader).

-spec handler_init(#state{}, Req, any())
	-> {ok, Req, cowboy_middleware:env()} |
    {suspend, module(), atom(), [any()]}
	when Req::cowboy_req:req().
handler_init(State=#state{env=Env, transport=Transport,
		handler=Handler}, Req, HandlerOpts) ->
	try Handler:websocket_init(Transport:name(), Req, HandlerOpts) of
		{ok, Req2, HandlerState} ->
			websocket_handshake(State, Req2, HandlerState);
		{ok, Req2, HandlerState, hibernate} ->
			websocket_handshake(State#state{hibernate=true},
				Req2, HandlerState);
		{ok, Req2, HandlerState, Timeout} ->
			websocket_handshake(State#state{timeout=Timeout},
				Req2, HandlerState);
		{ok, Req2, HandlerState, Timeout, hibernate} ->
			websocket_handshake(State#state{timeout=Timeout,
				hibernate=true}, Req2, HandlerState);
		{shutdown, Req2} ->
			cowboy_req:ensure_response(Req2, 400),
			{ok, Req2, [{result, closed}|Env]}
	catch Class:Reason ->
		Stacktrace = erlang:get_stacktrace(),
		cowboy_req:maybe_reply(Stacktrace, Req),
		erlang:Class([
			{reason, Reason},
			{mfa, {Handler, websocket_init, 3}},
			{stacktrace, Stacktrace},
			{req, cowboy_req:to_list(Req)},
			{opts, HandlerOpts}
		])
	end.

-spec websocket_handshake(#state{}, Req, any()) ->
	{ok, Req, cowboy_middleware:env()} | {suspend, module(), atom(), [any()]}
	when Req :: cowboy_req:req().
websocket_handshake(State=#state{transport=Transport, key=Key}, Req, HandlerState) ->
	Challenge = base64:encode(crypto:hash(sha,
		<< Key/binary, "258EAFA5-E914-47DA-95CA-C5AB0DC85B11" >>)),
	{ok, Req2} = cowboy_req:upgrade_reply(
		101,
		[{<<"upgrade">>, <<"websocket">>},
		 {<<"sec-websocket-accept">>, Challenge}],
		Req),
	%% Flush the resp_sent message before moving on.
	receive {cowboy_req, resp_sent} -> ok after 0 -> ok end,
	State2 = handler_loop_timeout(State),
	handler_before_loop(State2#state{key=undefined,
		messages=Transport:messages()}, Req2, HandlerState, <<>>).

-spec handler_before_loop(#state{}, Req, any(), binary())
	-> {ok, Req, cowboy_middleware:env()}
	| {suspend, module(), atom(), [any()]}
	when Req::cowboy_req:req().
handler_before_loop(State=#state{
			socket=Socket, transport=Transport, hibernate=true},
		Req, HandlerState, SoFar) ->
	Transport:setopts(Socket, [{active, once}]),
	{suspend, ?MODULE, handler_loop,
		[State#state{hibernate=false}, Req, HandlerState, SoFar]};
handler_before_loop(State=#state{socket=Socket, transport=Transport},
		Req, HandlerState, SoFar) ->
	Transport:setopts(Socket, [{active, once}]),
	handler_loop(State, Req, HandlerState, SoFar).

-spec handler_loop_timeout(#state{}) -> #state{}.
handler_loop_timeout(State=#state{timeout=infinity}) ->
	State#state{timeout_ref=undefined};
handler_loop_timeout(State=#state{timeout=Timeout, timeout_ref=PrevRef}) ->
	_ = case PrevRef of undefined -> ignore; PrevRef ->
		erlang:cancel_timer(PrevRef) end,
	TRef = erlang:start_timer(Timeout, self(), ?MODULE),
	State#state{timeout_ref=TRef}.

-spec handler_loop(#state{}, Req, any(), binary())
	-> {ok, Req, cowboy_middleware:env()}
	| {suspend, module(), atom(), [any()]}
	when Req::cowboy_req:req().
handler_loop(State=#state{socket=Socket, messages={OK, Closed, Error},
		timeout_ref=TRef}, Req, HandlerState, SoFar) ->
	receive
		{OK, Socket, Data} ->
			State2 = handler_loop_timeout(State),
			websocket_data(State2, Req, HandlerState,
				<< SoFar/binary, Data/binary >>);
		{Closed, Socket} ->
			handler_terminate(State, Req, HandlerState, {error, closed});
		{Error, Socket, Reason} ->
			handler_terminate(State, Req, HandlerState, {error, Reason});
		{timeout, TRef, ?MODULE} ->
			websocket_close(State, Req, HandlerState, timeout);
		{timeout, OlderTRef, ?MODULE} when is_reference(OlderTRef) ->
			handler_loop(State, Req, HandlerState, SoFar);
		Message ->
			handler_call(State, Req, HandlerState,
				SoFar, websocket_info, Message, fun handler_before_loop/4)
	end.

-spec websocket_data(#state{}, Req, any(), binary()) ->
    {ok, Req, cowboy_middleware:env()} | {suspend, module(), atom(), [any()]}
    when Req::cowboy_req:req().
websocket_data(State=#state{frag_state=FragState, extensions=Extensions},
    Req, HandlerState, Data) ->
    case cow_ws:parse_header(Data, Extensions, FragState) of
        {_Type, _FragState2, _Rsv, _Len, undefined, _Rest} ->
            websocket_close(State, Req, HandlerState, {error, badframe});
        {Type, FragState2, Rsv, Len, MaskKey, Rest} ->
            websocket_payload(State#state{frag_state=FragState2},
                Req, HandlerState, Type, Len, MaskKey,
                Rsv, undefined, <<>>, 0, Rest);
        more ->
            handler_before_loop(State, Req, HandlerState, Data);
        error ->
            websocket_close(State, Req, HandlerState, {error, badframe})
    end.

-spec websocket_payload(#state{}, cowboy_req:req(), any(), cow_ws:frame_type(),
    non_neg_integer(), cow_ws:mask_key(), cow_ws:rsv(), cow_ws:close_code(),
    binary(), non_neg_integer(), binary()) ->
    {ok, cowboy_req:req(), cowboy_middleware:env()} |
    {suspend, module(), atom(), [any()]}.
websocket_payload(State=#state{
    frag_state=FragState, utf8_state=Incomplete, extensions=Extensions},
    Req, HandlerState, Type, Len, MaskKey, Rsv, CloseCode, Unmasked,
    UnmaskedLen, Data) ->
    case cow_ws:parse_payload(Data, MaskKey, Incomplete,
        UnmaskedLen, Type, Len, FragState, Extensions, Rsv) of
        {ok, CloseCode2, Payload, Utf8State, Rest} ->
            websocket_dispatch(State#state{utf8_state=Utf8State},
                Req, HandlerState, Type, <<Unmasked/binary, Payload/binary>>,
                CloseCode2, Rest);
        {ok, Payload, Utf8State, Rest} ->
            websocket_dispatch(State#state{utf8_state=Utf8State},
                Req, HandlerState, Type, <<Unmasked/binary, Payload/binary>>,
                CloseCode, Rest);
        {more, CloseCode2, Payload, Utf8State} ->
            websocket_payload_loop(State#state{utf8_state=Utf8State},
                Req, HandlerState, Type, Len - byte_size(Data), MaskKey, Rsv,
                CloseCode2, <<Unmasked/binary, Payload/binary>>,
                UnmaskedLen + byte_size(Data));
        {more, Payload, Utf8State} ->
            websocket_payload_loop(State#state{utf8_state=Utf8State},
                Req, HandlerState, Type, Len - byte_size(Data),
                MaskKey, Rsv, CloseCode,
                << Unmasked/binary, Payload/binary >>,
                UnmaskedLen + byte_size(Data));
        Error = {error, _Reason} ->
            websocket_close(State, Req, HandlerState, Error)
    end.

-spec websocket_payload_loop(#state{}, cowboy_req:req(), any(),
    cow_ws:frame_type(), non_neg_integer(), cow_ws:mask_key(),
    cow_ws:rsv(), cow_ws:close_code(), binary(), non_neg_integer()) ->
    {ok, cowboy_req:req(), cowboy_middleware:env()} |
    {suspend, module(), atom(), [any()]}.
websocket_payload_loop(State=#state{socket=Socket, transport=Transport,
    messages={OK, Closed, Error}, timeout_ref=TRef},
    Req, HandlerState, Type, Len, MaskKey, Rsv,
    CloseCode, Unmasked, UnmaskedLen) ->
    Transport:setopts(Socket, [{active, once}]),
    receive
        {OK, Socket, Data} ->
            State2 = handler_loop_timeout(State),
            websocket_payload(State2, Req, HandlerState,
                Type, Len, MaskKey, Rsv, CloseCode,
                Unmasked, UnmaskedLen, Data);
        {Closed, Socket} ->
            handler_terminate(State, Req, HandlerState, {error, closed});
        {Error, Socket, Reason} ->
            handler_terminate(State, Req, HandlerState, {error, Reason});
        {timeout, TRef, ?MODULE} ->
            websocket_close(State, Req, HandlerState, timeout);
        {timeout, OlderTRef, ?MODULE} when is_reference(OlderTRef) ->
            websocket_payload_loop(State, Req, HandlerState,
                Type, Len, MaskKey, Rsv, CloseCode,
                Unmasked, UnmaskedLen);
        Message ->
            handler_call(State, Req, HandlerState,
                <<>>, websocket_info, Message,
                fun (State2, Req2, HandlerState2, _) ->
                    websocket_payload_loop(State2, Req2, HandlerState2,
                        Type, Len, MaskKey, Rsv, CloseCode, Unmasked,
                        UnmaskedLen)
                end)
    end.

-spec websocket_dispatch(#state{}, cowboy_req:req(), any(),
    cow_ws:frame_type(), binary(), cow_ws:close_code(), binary()) ->
    {ok, cowboy_req:req(), cowboy_middleware:env()} |
    {suspend, module(), atom(), [any()]}.
websocket_dispatch(State=#state{socket=Socket, transport=Transport,
    frag_state=FragState, frag_buffer=SoFar, extensions=Extensions},
    Req, HandlerState, Type0, Payload0, CloseCode0, RemainingData) ->
    case cow_ws:make_frame(Type0, Payload0, CloseCode0, FragState) of
        %% @todo Allow receiving fragments.
        {fragment, nofin, _, Payload} ->
            websocket_data(State#state{frag_buffer= << SoFar/binary,
                Payload/binary >>}, Req, HandlerState, RemainingData);
        {fragment, fin, Type, Payload} ->
            handler_call(State#state{frag_state=undefined, frag_buffer= <<>>},
                Req, HandlerState, RemainingData,
                websocket_handle, {Type, << SoFar/binary, Payload/binary >>},
                fun websocket_data/4);
        close ->
            websocket_close(State, Req, HandlerState,
                remote);
        {close, CloseCode, Payload} ->
            websocket_close(State, Req, HandlerState,
                {remote, CloseCode, Payload});
        Frame = ping ->
            Transport:send(Socket, cow_ws:frame(pong, Extensions)),
            handler_call(State, Req, HandlerState, RemainingData,
                websocket_handle, Frame, fun websocket_data/4);
        Frame = {ping, Payload} ->
            Transport:send(Socket, cow_ws:frame({pong, Payload}, Extensions)),
            handler_call(State, Req, HandlerState, RemainingData,
                websocket_handle, Frame, fun websocket_data/4);
        Frame ->
            handler_call(State, Req, HandlerState, RemainingData,
                websocket_handle, Frame, fun websocket_data/4)
    end.

-spec handler_call(#state{}, Req, any(), binary(), atom(), any(), fun())
	-> {ok, Req, cowboy_middleware:env()}
	| {suspend, module(), atom(), [any()]}
	when Req::cowboy_req:req().
handler_call(State=#state{handler=Handler}, Req, HandlerState,
		RemainingData, Callback, Message, NextState) ->
	try Handler:Callback(Message, Req, HandlerState) of
		{ok, Req2, HandlerState2} ->
			NextState(State, Req2, HandlerState2, RemainingData);
		{ok, Req2, HandlerState2, hibernate} ->
			NextState(State#state{hibernate=true},
				Req2, HandlerState2, RemainingData);
		{reply, Payload, Req2, HandlerState2} when is_list(Payload) ->
			case websocket_send_many(Payload, State) of
				ok ->
					NextState(State, Req2, HandlerState2, RemainingData);
				stop ->
					handler_terminate(State, Req2, HandlerState2, stop);
				Error = {error, _} ->
					handler_terminate(State, Req2, HandlerState2, Error)
			end;
		{reply, Payload, Req2, HandlerState2, hibernate} when is_list(Payload) ->
			case websocket_send_many(Payload, State) of
				ok ->
					NextState(State#state{hibernate=true},
						Req2, HandlerState2, RemainingData);
				stop ->
					handler_terminate(State, Req2, HandlerState2, stop);
				Error = {error, _} ->
					handler_terminate(State, Req2, HandlerState2, Error)
			end;
		{reply, Payload, Req2, HandlerState2} ->
			case websocket_send(Payload, State) of
				ok ->
					NextState(State, Req2, HandlerState2, RemainingData);
                stop ->
					handler_terminate(State, Req2, HandlerState2, stop);
				Error = {error, _} ->
					handler_terminate(State, Req2, HandlerState2, Error)
			end;
		{reply, Payload, Req2, HandlerState2, hibernate} ->
			case websocket_send(Payload, State) of
				ok ->
					NextState(State#state{hibernate=true},
						Req2, HandlerState2, RemainingData);
				stop ->
					handler_terminate(State, Req2, HandlerState2, stop);
				Error = {error, _} ->
					handler_terminate(State, Req2, HandlerState2, Error)
			end;
		{shutdown, Req2, HandlerState2} ->
			websocket_close(State, Req2, HandlerState2, stop)
	catch Class:Reason ->
		_ = websocket_close(State, Req, HandlerState, {error, handler}),
		erlang:Class([
			{reason, Reason},
			{mfa, {Handler, Callback, 3}},
			{stacktrace, erlang:get_stacktrace()},
			{msg, Message},
			{req, cowboy_req:to_list(Req)},
			{state, HandlerState}
		])
	end.

-spec websocket_send(cow_ws:frame(), #state{}) -> ok | stop | {error, atom()}.
websocket_send(Frame, #state{socket=Socket, transport=Transport,
    extensions=Extensions}) ->
    Res = Transport:send(Socket, cow_ws:frame(Frame, Extensions)),
    case Frame of
        close -> stop;
        {close, _} -> stop;
        {close, _, _} -> stop;
        _ -> Res
    end.

-spec websocket_send_many([cow_ws:frame()], #state{})
	-> ok | stop | {error, atom()}.
websocket_send_many([], _State) ->
	ok;
websocket_send_many([Frame|Tail], State) ->
	case websocket_send(Frame, State) of
		ok -> websocket_send_many(Tail, State);
		stop -> stop;
		Error -> Error
	end.

-spec websocket_close(#state{}, Req, any(), terminate_reason())
        -> {ok, Req, cowboy_middleware:env()}
    when Req::cowboy_req:req().
websocket_close(State=#state{socket=Socket, transport=Transport,
    extensions=Extensions},
    Req, HandlerState, Reason) ->
    case Reason of
        Normal when Normal =:= stop; Normal =:= timeout ->
            Transport:send(Socket, cow_ws:frame({close, 1000, <<>>},
                Extensions));
        {error, badframe} ->
            Transport:send(Socket, cow_ws:frame({close, 1002, <<>>},
                Extensions));
        {error, badencoding} ->
            Transport:send(Socket, cow_ws:frame({close, 1007, <<>>},
                Extensions));
        {error, handler} ->
            Transport:send(Socket, cow_ws:frame({close, 1011, <<>>},
                Extensions));
        remote ->
            Transport:send(Socket, cow_ws:frame(close, Extensions));
        {remote, Code, _} ->
            Transport:send(Socket, cow_ws:frame({close, Code, <<>>},
                Extensions))
    end,
    handler_terminate(State, Req, HandlerState, Reason).

-spec handler_terminate(#state{}, Req, any(), terminate_reason())
	-> {ok, Req, cowboy_middleware:env()}
	when Req::cowboy_req:req().
handler_terminate(#state{env=Env, handler=Handler},
		Req, HandlerState, TerminateReason) ->
	try
		Handler:websocket_terminate(TerminateReason, Req, HandlerState)
	catch Class:Reason ->
		erlang:Class([
			{reason, Reason},
			{mfa, {Handler, websocket_terminate, 3}},
			{stacktrace, erlang:get_stacktrace()},
			{req, cowboy_req:to_list(Req)},
			{state, HandlerState},
			{terminate_reason, TerminateReason}
		])
	end,
	{ok, Req, [{result, closed}|Env]}.
