import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html

import data_assets.style as css
import data_assets.sections as sct

def global_stats_elements(stats, section):
    if section != None:
        section = '-'.join(section)
    else: 
        section = ''

    stats_list = []
    element_wrapper = []
    for single_stat in stats:
        temp_list = []
        if single_stat in ['Track Temp.']:
            for single_element in ['Air Temp.', 'Track Temp.']:
                temp_list.append(
                    html.Div(
                        [
                            html.P(single_element.replace('_', ' ').title()),
                            html.H3(stats[single_element])
                        ],
                        id='temperature-div'
                    )
                )
        elif single_stat in ['Air Temp.']: 
            pass
        else:
            temp_list.append(
            html.P(single_stat.replace('_', ' ').title())
            )
            if list(stats.keys()).index(single_stat) < 3 and single_stat != 'Track Length':
                temp_list.append(
                    html.H2(stats[single_stat])
                )
            else:
                temp_list.append(
                    html.H3(stats[single_stat])
                )
        if len(temp_list) > 0:
            element_wrapper.append(temp_list)

    for single_element in element_wrapper:
        stats_list.append(
            html.Div(
                single_element,
                className='global-stats'
                )
        )

    return stats_list

def header_bar(stats = None, pathname = None):
    if pathname != None:
        pathname_splitted = pathname[1:].split('-')
    else:
        pathname_splitted = pathname

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
                global_stats_elements(
                    stats, 
                    section = pathname_splitted
                    ),
                id='global-stats-wrapper'
            )
        ],
        id='header'
    )
    
    return headbar

def header_bar_session(stats = None, pathname = None, sessionUID = None):

    pathname_splitted = pathname[1:].split('-')

    session_row = stats['recent_statistics_df'][stats['recent_statistics_df']['sessionUID'] == sessionUID].to_dict('records')[0]
    
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
        'bottom': {}
    }

    session_stats_groups['top']['Session Time Driven'] = session_row['sessionTime_format']
    session_stats_groups['top']['Session Distance Driven'] = session_row['distance_driven_format']
    session_stats_groups['top']['Session Datapoints Count'] = session_row['datapoint_count_format']
    session_stats_groups['track']['Track Preview'] = 'assets/images/tracks/{}.svg'.format(session_row['track'])
    session_stats_groups['bottom']['Air Temp.'] = '{}°C'.format(session_row['airTemperature'])
    session_stats_groups['bottom']['Track Temp.'] = '{}°C'.format(session_row['trackTemperature'])

    session_stats_groups['bottom']['Track Length'] = '{:,}km'.format(
        session_row['trackLength'] / 1000
        ).replace(',', ' ')
    session_stats_groups['bottom']['Your Fastest Lap Record'] = session_row['record_lap_format']
    session_stats_groups['bottom']['Weather'] = weather_type

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
                            html.Div(id="session-indicator2"),
                        ],
                        id='title-wrapper-top'
                    ), 
                    html.Div(
                        global_stats_elements(
                            session_stats_groups['top'], 
                            section = pathname_splitted
                            ),
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
                        global_stats_elements(
                            session_stats_groups['bottom'], 
                            section = pathname_splitted
                            ),
                        id='global-stats-wrapper'
                    )
                ],
                id='header-bottom-row'
            ),
        ],
        id='header-session'
    )
    
    return headbar
