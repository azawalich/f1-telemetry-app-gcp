import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State

import data_assets.style as css
import data_assets.sections as sct

import modules.get_data as mdl_get_data
import modules.navigation as mdl_navigation
import modules.header as mdl_header
import modules.homepage as mdl_homepage

ext_styles = [dbc.themes.BOOTSTRAP]

app = dash.Dash(
    external_stylesheets=ext_styles,
    suppress_callback_exceptions=True,
    meta_tags=[
        {
            'property': 'og:title',
            'content': 'F1 Telemetry App - Aleksander Zawalich'
        },
        {
            'property': 'og:description',
            'content': 'F1 telemetry app hosted on GCP'
        },
        {
            'property': 'og:image',
            'content': 'https://www.codemasters.com/wp-content/uploads/2019/03/f1_2019_monza_010.jpg'
        },
        {
            'property': 'og:url',
            'content': 'http://f1.zawalich.pl'
        },
        {
            'property': 'og:type',
            'content': 'website'
        },
        {
            'http-equiv': 'X-UA-Compatible',
            'content': 'IE=edge'
        },
        {
            'charset': 'UTF-8'
        },
    ]
    )

server = app.server

stats_data = mdl_get_data.overall_stats()

sidebar = html.Div(
    id='sidebar'
    )

content = html.Div(
    id='page-content'
    )

header = html.Div(
    id='header-wrapper'
)

def serve_layout():
    layout = html.Div(
        html.Div(
        [
            dcc.Location(id="url"),
            sidebar,
            header,
            content,
            html.Div(0, id='paging-state-default', style={'display': 'none'}),
            html.Div('All Sessions', id='choice-default', style={'display': 'none'}),
            html.Div(id='paging-state', style={'display': 'none'}),
            html.Div(id='choice', style={'display': 'none'})
        ],
        id = 'layout-design',
        className = 'layout-design'
        ),
        id = 'scaleable-wrapper',
        className = 'scaleable-wrapper'
    )
    
    return layout

app.layout = serve_layout

@app.callback(Output("sidebar", "children"), [Input("url", "pathname")])
def render_navigation_content(pathname):
    if pathname not in [None, '/', '/homepage']:
        pathname_clean, publish_time = pathname.split('%3F')
        publish_time = publish_time.replace('%20', ' ')
    else:
        pathname_clean = pathname

    if pathname_clean in ['/session-summary']:
        return mdl_navigation.navigation_bar_links(stats = stats_data, publish_time = publish_time)
    else:
        return mdl_navigation.navigation_bar(stats = stats_data['global_records'])

@app.callback(Output("header-wrapper", "children"), [Input("url", "pathname")])
def render_header_content(pathname):
    if pathname not in [None, '/', '/homepage']:
        pathname_clean, publish_time = pathname.split('%3F')
        publish_time = publish_time.replace('%20', ' ')
    else:
        pathname_clean = pathname
    
    if pathname_clean in ['/session-summary']:
        return mdl_header.header_bar_session(stats = stats_data, pathname = pathname_clean, publish_time = publish_time)
    else:
        return mdl_header.header_bar(stats = stats_data['global_statistics'])

@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def render_page_content(pathname):
    if pathname not in [None, '/', '/homepage']:
        pathname_clean, publish_time = pathname.split('%3F')
        publish_time = publish_time.replace('%', '')
    else:
        pathname_clean = pathname
    
    if pathname_clean in ["/", "/homepage"]:
        return mdl_homepage.homepage_wrapper(stats = stats_data, page_size = 10)
    elif pathname_clean == "/session-summary":
        return html.P("This is the content of page 2. Yay!")
    # If the user tries to reach a different page, return a 404 message
    return html.Div(
        [
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname_clean} was not recognised..."),
            html.A('Back to homepage', href='/'),
        ]
    )

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
        sessionUID = dff_return['Session Time'].tolist()[active_cell['row']]
        output_session = dcc.Location(pathname="/session-summary?publish_time={}".format(sessionUID), id="redirect-id")

    return dff_return.to_dict('records'), pages_count, output_session, eval('a1'), eval('a2'), eval('a3'), eval('a4'), eval('a5'), eval('a6'), eval('a7')

@app.callback(
    [   
        Output('session-summary', 'className'),
        Output('driver-ranking', 'className'),
        Output('sector-data', 'className'),
        Output('session-map', 'className'),
        Output('telemetry-data', 'className'),
        Output('tires-age', 'className'),
        Output('pure-pitwall', 'className')
    ], 
    [Input("url", "pathname")])
def render_active_menu(pathname):
    if pathname not in [None, '/', '/homepage']:
        pathname_clean = pathname.split('%3F')[0]
    else:
        pathname_clean = pathname

    for single_indeks in range(1, len(sct.SECTIONS.keys())):
        single_section = list(sct.SECTIONS.keys())[single_indeks]
        if single_section == pathname_clean[1:]:
            exec('a{}="navigation-link-current"'.format(single_indeks))
        else:
            exec('a{}=""'.format(single_indeks))
    return eval('a1'), eval('a2'), eval('a3'), eval('a4'), eval('a5'), eval('a6'), eval('a7')

@app.callback(
    [   
        Output('session-summary-current-image', 'className'),
        Output('driver-ranking-current-image', 'className'),
        Output('sector-data-current-image', 'className'),
        Output('session-map-current-image', 'className'),
        Output('telemetry-data-current-image', 'className'),
        Output('tires-age-current-image', 'className'),
        Output('pure-pitwall-current-image', 'className')
    ], 
    [Input("url", "pathname")])
def render_active_menu_image(pathname):
    if pathname not in [None, '/', '/homepage']:
        pathname_clean = pathname.split('%3F')[0]
    else:
        pathname_clean = pathname

    for single_indeks in range(1, len(sct.SECTIONS.keys())):
        single_section = list(sct.SECTIONS.keys())[single_indeks]
        if single_section == pathname_clean[1:]:
            exec('a{}="navigation-link-current-image"'.format(single_indeks))
        else:
            exec('a{}=""'.format(single_indeks))
    return eval('a1'), eval('a2'), eval('a3'), eval('a4'), eval('a5'), eval('a6'), eval('a7')

if __name__ == "__main__":
    app.title = 'F1 Telemetry App - Aleksander Zawalich'
    app.run_server('0.0.0.0', port=5005)