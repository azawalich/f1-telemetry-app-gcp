from dash.dependencies import Input, Output
import dash_core_components as dcc

from app import app
import functions.get_data as fct_get_data
import functions.callback_functions as fct_call_fct

stats_data = fct_get_data.overall_stats()
participants_data_call = None

@app.callback(Output("sidebar", "children"), [Input("url", "pathname")])
def render_navigation_content(pathname):
    return fct_call_fct.return_dash_content(pathname, 'navigation', stats_data)

@app.callback(Output("header-wrapper", "children"), [Input("url", "pathname")])
def render_header_content(pathname):
    return fct_call_fct.return_dash_content(pathname, 'header', stats_data)

@app.callback(Output("page-content-wrapper", "children"), [Input("url", "pathname")])
def render_page_content(pathname):
    if pathname != None:
        if '/session-summary' in pathname:
            global participants_data_call
            elements_list, participants_data_call = fct_call_fct.return_dash_content(pathname, 'page_content', stats_data)
            return elements_list
        else:
            return fct_call_fct.return_dash_content(pathname, 'page_content', stats_data)

@app.callback(
    [   
        Output('session-summary', 'className'),
        Output('session-laps', 'className'),
        Output('driver-ranking', 'className'),
        Output('session-map', 'className'),
        Output('telemetry-data', 'className'),
        Output('tires-age', 'className'),
        Output('pure-pitwall', 'className'),
    ], 
    [Input("url", "pathname")])
def render_active_menu(pathname):
    return fct_call_fct.return_active_class_elements(pathname, 'active_menu')

@app.callback(
    [   
        Output('session-summary', 'href'),
        Output('session-laps', 'href'),
        Output('driver-ranking', 'href'),
        Output('session-map', 'href'),
        Output('telemetry-data', 'href'),
        Output('tires-age', 'href'),
        Output('pure-pitwall', 'href')
    ], 
    [Input("url", "pathname")])
def render_navigation_links(pathname):
    return fct_call_fct.return_navigation_links(pathname)

@app.callback(
    [   
        Output('session-summary-current-image', 'className'),
        Output('session-laps-current-image', 'className'),
        Output('driver-ranking-current-image', 'className'),
        Output('session-map-current-image', 'className'),
        Output('telemetry-data-current-image', 'className'),
        Output('tires-age-current-image', 'className'),
        Output('pure-pitwall-current-image', 'className')
    ], 
    [Input("url", "pathname")])
def render_active_menu_image(pathname):
    return fct_call_fct.return_active_class_elements(pathname, 'active_menu_image')

sorters_state = dict.fromkeys(['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7'])
sorters_names = list(stats_data['session_cards'].keys())

@app.callback(
    [
        Output('datatable-paging-page-count', 'filter_query'),
        Output('datatable-paging-page-count', 'active_cell'),
        Output('datatable-paging-page-count', 'page_current')
    ],
    [
        Input('All Sessions', 'n_clicks'),
        Input('Online', 'n_clicks'),
        Input('Offline', 'n_clicks'),
        Input('Time Trial', 'n_clicks'),
        Input('Practice', 'n_clicks'),
        Input('Qualifications', 'n_clicks'),
        Input('Race', 'n_clicks')
    ]
    )
def update_filter_query(a1, a2, a3, a4, a5, a6, a7):
    
    if len(set([a1, a2, a3, a4, a5, a6, a7])) == 1:
        choice = 'All Sessions'
    else:
        for state_indeks in range(0, len(list(sorters_state.keys()))):
            single_state = list(sorters_state.keys())[state_indeks]
            if sorters_state[single_state] != eval(single_state):
                sorters_state[single_state] = eval(single_state)
                choice = sorters_names[state_indeks]

    return choice, None, 0

@app.callback(
    [
        Output('datatable-paging-page-count', 'data'),
        Output('datatable-paging-page-count', 'page_count'),
        Output("session-indicator", "children"),
        Output('All Sessions', 'className'),
        Output('Online', 'className'),
        Output('Offline', 'className'),
        Output('Time Trial', 'className'),
        Output('Practice', 'className'),
        Output('Qualifications', 'className'),
        Output('Race', 'className')
    ],
    [
        Input('datatable-paging-page-count', 'active_cell'),
        Input('datatable-paging-page-count', "page_current"),
        Input('datatable-paging-page-count', "page_size"),
        Input('datatable-paging-page-count', "filter_query")
    ]
    )
def update_sessions_table(active_cell, page_current, page_size, filter_query):
    
    for single_indeks in range(0, len(stats_data['session_cards'])):
        single_type = list(stats_data['session_cards'].keys())[single_indeks]

        if stats_data['session_cards'][single_type]['count'] == 0:
            exec('a{}="inactive"'.format(single_indeks+1))
        else:
            if single_type == filter_query:
                exec('a{}="active"'.format(single_indeks+1))
            else:
                exec('a{}="to-choose"'.format(single_indeks+1))

    dff = stats_data['choice_table']
    dff = dff.iloc[stats_data['session_cards'][filter_query]['rows']]

    dff['Id'] = range(1, len(dff) + 1)
    dff['Id'] = dff['Id'].astype(str) + '.'

    pages_count = int(round(dff.shape[0] / page_size, 0))

    if dff.shape[0] < page_size:
        pages_count = 1
    else:
        if pages_count == 0:
            pages_count = 1
        else:
            pages_count += 1

    dff_return = dff.iloc[
        page_current*page_size:(page_current+ 1)*page_size
    ]

    if active_cell == None:
        output_session = 'None'
    else:
        sessionUID = stats_data['recent_statistics_df']['sessionUID'].tolist()[active_cell['row']]
        output_session = dcc.Location(pathname="/session-summary?sessionUID={}".format(sessionUID), id="redirect-id")

    return dff_return.to_dict('records'), pages_count, output_session, eval('a1'), eval('a2'), eval('a3'), eval('a4'), eval('a5'), eval('a6'), eval('a7')

@app.callback(
    [
        Output("session-indicator2", "children")
    ],
    [
        Input('datatable-2-paging-page-count', 'active_cell'),
        Input("url", "pathname")
    ]
    )
def redirect_sessions_2_table(active_cell, pathname):

    if active_cell == None:
        output_session = 'None'
    else:
        sessionUID = pathname.split('=')[1]
        output_session = dcc.Location(pathname="/session-laps?sessionUID={}".format(sessionUID), id="redirect-id")

    return [output_session]

@app.callback(
    [
        Output('datatable-3-paging-page-count', 'data')
    ],
    [
        Input('datatable-3-paging-page-count', 'active_cell'),
        Input('datatable-3-paging-page-count', "page_current"),
        Input('datatable-3-paging-page-count', "page_size")
    ]
    )
def update_sessions_3_table(active_cell, page_current, page_size):

    return [participants_data_call.iloc[
        page_current*page_size:(page_current+ 1)*page_size
    ].to_dict('records')]