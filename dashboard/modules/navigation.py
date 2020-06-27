import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html

import data_assets.style as css
import data_assets.sections as sct

navigation_elements = []

for single_section in sct.SECTIONS.keys():
    if single_section in ['homepage']:
        link_href = "/{}".format(single_section)
    else:
        link_href = ''
    # jak time trial, to nie pokazuj bezsensownych linków, np drivers ranking
    temp_section = sct.SECTIONS[single_section]
    element = html.Div(
        [
            html.A(
                [
                    html.Img(
                        src=temp_section['icon'],
                        alt=temp_section['name']
                    ),
                    temp_section['name'],
                ], 
                href=link_href,
                id=single_section
            ),
        html.Img(
            src='assets/images/menu_active.svg',
            id='{}-current-image'.format(single_section)
            ),
        ],
        id = 'link-wrapper'
    )
    navigation_elements.append(element)

def navigation_bar(stats = None):
    navbar = [
        html.Div(
            [
                html.Img(src="https://upload.wikimedia.org/wikipedia/commons/3/33/F1.svg"),
                html.Img(src="./assets/images/TELEMETRY.svg")
            ],
            id="logo"
        ),
        html.Div(
            [
                html.Img(src='./assets/images/azawalich.svg'),
                html.P(
                    [
                    'Aleksander', 
                    html.Br(),
                    html.B(
                        'Zawalich'
                        )
                    ]
                    )],
                id='user'
                ),
        html.Div(
            [
                html.P('Events Won'),
                html.B(stats['events_won']),
                html.Img(
                            src="https://raw.githubusercontent.com/FortAwesome/Font-Awesome/master/svgs/solid/trophy.svg"
                        )
            ],
            className='overall-stat'
            ),
        html.Div(
            [
                html.P('Fastest Lap Count'),
                html.B(stats['fastest_laps']),
                html.Img(
                            src="https://raw.githubusercontent.com/FortAwesome/Font-Awesome/master/svgs/solid/stopwatch.svg"
                        )
            ],
            className='overall-stat'
            ),
        html.Div(
            html.P(
                '''I am a data scientist successfully putting a psychology degree to use while 
                crunching data. I strongly enjoy programming and solving technical challenges. 
                I am also the enthusiast of cloud, machine learning and dockerization.'''
            ),
            id='description'
        ),
        html.Div(
            [
                html.A(
                    [
                        html.Img(
                            src="https://raw.githubusercontent.com/FortAwesome/Font-Awesome/master/svgs/brands/gitlab.svg"
                        ),
                        'GitLab'
                    ], 
                    href='https://gitlab.com/azawalich/f1-telemetry-app-gcp/', 
                    target='_blank'
                    ),
                    html.A(
                    [
                        html.Img(
                            src="https://raw.githubusercontent.com/FortAwesome/Font-Awesome/master/svgs/brands/linkedin.svg"
                        ),
                        'LinkedIn'
                    ], 
                    href='https://www.linkedin.com/in/aleksanderzawalich/', 
                    target='_blank'
                    )
            ],
            className='navigation-link'
            ),
        html.Div(
            [
                html.P(
                    [
                        'Powered with ', 
                        html.A(
                            'F1™® 2019', 
                            href='https://www.codemasters.com/game/f1-2019/', 
                            target='_blank'
                            ),
                        ' data by: ',
                        html.A(
                            'GCP', 
                            href='https://cloud.google.com', 
                            target='_blank'
                            ),
                        ' and ',
                        html.A(
                            'Dash', 
                            href='https://plotly.com/dash/', 
                            target='_blank'
                            ),
                    ]
        ),
        html.P(
            [
                'For feedback and inquiries, ', 
                html.A(
                    "let's get in touch ;)", 
                    href='mailto:aleksander@zawalich.pl'
                    )
            ]
        ),
        html.P(
            [
                'Dashboard code by ', 
                html.A(
                    'Aleksander Zawalich', 
                    href='http://zawalich.pl', 
                    target='_blank'
                    )
            ]
        ),
        html.P(
            [
                'Dashboard design by ', 
                html.A(
                    'Kacper Rzosiński', 
                    href='https://www.linkedin.com/in/gathspar/', 
                    target='_blank'
                    )
            ]
        ),
        html.P('All trademarks are property of their respective owners.')
        ],
            id='navigation-footer'
        )       
    ]
    return navbar

def navigation_bar_links(stats, sessionUID):
    publish_time = stats['recent_statistics_df'][stats['recent_statistics_df']['sessionUID'] == sessionUID]['publish_time'].tolist()[0]
    track_name = stats['recent_statistics_df'][stats['recent_statistics_df']['sessionUID'] == sessionUID]['track'].tolist()[0]
    track_name_splitted = track_name.split(' ')

    if len(track_name_splitted) > 1:
        session_info_html = [
            html.H2(
                [
                    track_name_splitted[0],
                    html.Span(
                        track_name_splitted[1],
                        style = {
                            'display': 'block'
                            }
                    )
                ]
            ),
            html.P(publish_time)
        ]
    else:
        session_info_html = [
            html.H2(track_name),
            html.P(publish_time)
        ]     

    navbar = [
        html.Div(
            [
                html.Img(src="https://upload.wikimedia.org/wikipedia/commons/3/33/F1.svg"),
                html.Img(src="./assets/images/TELEMETRY.svg")
            ],
            id="logo"
        ),
        html.Div(
            session_info_html,
            id='session-info'
        ),
        html.Div(
            html.Div(
                navigation_elements,
                className='navigation-link'
            ),
            id='menu'
        ),
        html.Div(
            [
                html.P(
                    [
                        'Powered with ', 
                        html.A(
                            'F1™® 2019', 
                            href='https://www.codemasters.com/game/f1-2019/', 
                            target='_blank'
                            ),
                        ' data by: ',
                        html.A(
                            'GCP', 
                            href='https://cloud.google.com', 
                            target='_blank'
                            ),
                        ' and ',
                        html.A(
                            'Dash', 
                            href='https://plotly.com/dash/', 
                            target='_blank'
                            ),
                    ]
        ),
        html.P(
            [
                'For feedback and inquiries, ', 
                html.A(
                    "let's get in touch ;)", 
                    href='mailto:aleksander@zawalich.pl'
                    )
            ]
        ),
        html.P(
            [
                'Dashboard code by ', 
                html.A(
                    'Aleksander Zawalich', 
                    href='http://zawalich.pl', 
                    target='_blank'
                    )
            ]
        ),
        html.P(
            [
                'Dashboard design by ', 
                html.A(
                    'Kacper Rzosiński', 
                    href='https://www.linkedin.com/in/gathspar/', 
                    target='_blank'
                    )
            ]
        ),
        html.P('All trademarks are property of their respective owners.')
        ],
            id='navigation-footer'
        ),
        html.Div(
            [
                html.Img(src='./assets/images/azawalich.svg'),
                html.P(
                    [
                    'Aleksander', 
                    html.Br(),
                    'Zawalich'
                    ]
                    )],
                id='user-small'
        )
    ]
    return navbar