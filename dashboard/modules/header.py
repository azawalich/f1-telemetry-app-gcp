import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html

import data_assets.style as css
import data_assets.sections as sct

navigation_elements = []

for single_section in sct.SECTIONS.keys():
    # jak time trial, to nie pokazuj bezsensownych linków, np drivers ranking
    temp_section = sct.SECTIONS[single_section]
    element = dbc.Row(
        [   
            html.Div(
            className="lead",
            id="{}-current-indicator".format(single_section)
                ),
            dbc.NavLink(
                temp_section['name'],
                href="/{}".format(single_section),
                id=single_section,
                style=temp_section['img_style'],
                disabled=temp_section['disabled'])
        ], align="center", no_gutters=True
    )
    navigation_elements.append(element)

def global_stats_elements(stats):
    stats_list = []
    element_wrapper = []
    for single_stat in stats:
        temp_list = []
        temp_list.append(
            html.P(single_stat.replace('_', ' ').title())
            )
        if single_stat == 'Weather':
            for single_key in stats[single_stat].keys():
                temp_list.append(
                    html.Div(
                        [
                            html.P(single_key.replace('_', ' ').title()),
                            html.H2(stats[single_stat][single_key])
                        ],
                        id='temperature-div'
                    )
                )
        else:
            if list(stats.keys()).index(single_stat) < 3:
                temp_list.append(
                    html.H2(stats[single_stat])
                )
            else:
                temp_list.append(
                    html.H3(stats[single_stat])
                )
        element_wrapper.append(temp_list)

    for single_element in element_wrapper:
        stats_list.append(
            html.Div(
                single_element,
                className='global-stats'
                )
        )

    return stats_list

def header_bar(stats = None):
    headbar = html.Div(
        [
            html.Div(
                [
                    html.H1(
                        [
                            'Global',
                            html.Span(
                                'Statistics',
                                style = {
                                    'display': 'block'
                                    }
                                )
                        ]
                ),
                html.Div(
                    id="session-indicator"
                ),
            ],
                id='title-wrapper'
            ), 
            html.Div(
                global_stats_elements(stats),
                id='global-stats-wrapper'
            )
        ],
        id='header'
    )
    
    return headbar

def header_bar_session(stats = None, pathname = None, publish_time = None):

    publish_time = publish_time.split('=')[1]
    pathname_splitted = pathname[1:].split('-')

    session_row = stats['recent_statistics_df'][stats['recent_statistics_df']['publish_time'] == publish_time].to_dict('records')[0]

    weather_types = {
        0: 'Clear',
        1: 'Light Cloud',
        2: 'Overcast',
        3: 'Light Rain',
        4: 'Heavy Rain',
        5: 'Storm',
    }

    weather_type = weather_types[session_row['weather']]

    session_stats_groups = {
        'top': {},
        'track': {},
        'bottom': {
            'Weather': {}
        }
    }

    session_stats_groups['top']['Session Time Driven'] = session_row['sessionTime_format']
    session_stats_groups['top']['Session Distance Driven'] = session_row['distance_driven_format']
    session_stats_groups['top']['Session Datapoints Count'] = session_row['datapoint_count_format']
    session_stats_groups['track']['Track Preview'] = 'assets/images/tracks/{}.svg'.format(session_row['track'])
    session_stats_groups['bottom']['Weather']['Air Temp.'] = '{}°C'.format(session_row['airTemperature'])
    session_stats_groups['bottom']['Weather']['Track Temp.'] = '{}°C'.format(session_row['trackTemperature'])

    session_stats_groups['bottom']['Track Length'] = '{:,}km'.format(
        session_row['trackLength'] / 1000
        ).replace(',', ' ')
    session_stats_groups['bottom']['Your Fastest Lap Record'] = session_row['record_lap_format']
    session_stats_groups['bottom'][weather_type] = html.Img(src='assets/images/weather/{}.svg'.format(weather_type))

    session_stats_groups['bottom']['Traction Control'] = session_row['assist_tractionControl']
    session_stats_groups['bottom']['Anti-Lock Brakes'] = session_row['assist_antiLockBrakes']
    
    if len(pathname_splitted) > 1:
        session_info_html_add = html.H1(
            [
                pathname_splitted[0].title(),
                html.Span(
                    pathname_splitted[1].title(),
                    style = {
                        'display': 'block'
                        }
                )
            ]
        )
    else:
        session_info_html_add = html.H1(pathname)
    
    headbar = html.Div(
        [   
            html.Div(
                html.Span(''),
                id='scroll-background'
            ),
            html.Div(
                [
                    html.Div(
                        [   
                            session_info_html_add,
                            html.Div(id="session-indicator"),
                        ],
                        id='title-wrapper-top'
                    ), 
                    html.Div(
                        global_stats_elements(session_stats_groups['top']),
                        id='global-stats-wrapper'
                    )
                ],
                id='header-top-row'
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.P('Track Preview'),
                            html.Img(src=session_stats_groups['track']['Track Preview'])
                        ],
                        id='title-wrapper-bottom'
                    ), 
                    html.Div(
                        global_stats_elements(session_stats_groups['bottom']),
                        id='global-stats-wrapper'
                    )
                ],
                id='header-bottom-row'
            ),
        ],
        id='header-session'
    )
    
    return headbar
