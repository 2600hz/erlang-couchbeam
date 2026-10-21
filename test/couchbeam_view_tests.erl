-module(couchbeam_view_tests).

-include_lib("eunit/include/eunit.hrl").
-include("couchbeam.hrl").

fetch_mode_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [fun default_fetch_is_synchronous/0,
      fun async_fetch_is_opt_in/0,
      fun asynchronous_partial_error/0,
      fun synchronous_request_error/0]}.

setup() ->
    meck:new([couchbeam_httpc, couchbeam_view_stream, kz_log], [non_strict, no_link]),
    meck:expect(couchbeam_httpc, server_url, fun(_) -> <<"http://localhost:5984">> end),
    meck:expect(couchbeam_httpc, db_url, fun(_) -> <<"accounts">> end),
    meck:expect(couchbeam_httpc, db_request,
                fun(get, _, [], <<>>, [], [200]) -> {ok, 200, [], response_ref} end),
    meck:expect(couchbeam_httpc, json_body,
                fun(response_ref) -> {[{<<"rows">>, [row()]}]} end),
    meck:expect(kz_log, get_callid, fun() -> <<"00000000000">> end),
    meck:expect(couchbeam_view_stream, start_link,
                fun(To, Ref, _Request, _Options) ->
                    {ok, spawn_link(fun() ->
                        To ! {Ref, {row, row()}},
                        To ! {Ref, done},
                        receive stop -> ok end
                    end)}
                end),
    {ok, Sup} = couchbeam_view_sup:start_link(),
    unlink(Sup),
    Sup.

cleanup(Sup) ->
    Ref = monitor(process, Sup),
    exit(Sup, shutdown),
    receive {'DOWN', Ref, process, Sup, _} -> ok after 1000 -> error(supervisor_alive) end,
    meck:unload([couchbeam_httpc, couchbeam_view_stream, kz_log]).

default_fetch_is_synchronous() ->
    ?assertEqual({ok, [row()]}, couchbeam_view:fetch(#db{}, all_docs,
                                                 [{stable, true}, {update, false}])),
    ?assertEqual(0, meck:num_calls(couchbeam_view_stream, start_link, '_')),
    Url = meck:capture(first, couchbeam_httpc, db_request, '_', 2),
    ?assertNotEqual(nomatch, binary:match(Url, <<"stable=true">>)),
    ?assertNotEqual(nomatch, binary:match(Url, <<"update=false">>)).

async_fetch_is_opt_in() ->
    ?assertEqual({ok, [row()]}, couchbeam_view:fetch(#db{}, all_docs, [async_query])),
    ?assertEqual(1, meck:num_calls(couchbeam_view_stream, start_link, '_')),
    ?assertEqual(0, meck:num_calls(couchbeam_httpc, db_request, '_')).

synchronous_request_error() ->
    meck:expect(couchbeam_httpc, db_request,
                fun(get, _, [], <<>>, [], [200]) -> {error, not_found} end),
    ?assertEqual({error, not_found}, couchbeam_view:fetch(#db{}, all_docs, [])),
    ?assertEqual(0, meck:num_calls(couchbeam_view_stream, start_link, '_')).

asynchronous_partial_error() ->
    meck:expect(couchbeam_view_stream, start_link,
                fun(To, Ref, _Request, _Options) ->
                    {ok, spawn_link(fun() ->
                        To ! {Ref, {row, row()}},
                        To ! {Ref, {error, timeout}},
                        receive stop -> ok end
                    end)}
                end),
    ?assertEqual({error, timeout, [row()]},
                 couchbeam_view:fetch(#db{}, all_docs, [async_query])).

row() ->
    {[{<<"id">>, <<"account">>}, {<<"key">>, <<"account">>}, {<<"value">>, 1}]}.
